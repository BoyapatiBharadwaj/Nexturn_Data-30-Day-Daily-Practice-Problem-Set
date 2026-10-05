import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

case class EcommerceEvent(
  eventId: String,
  customerId: String,
  productId: String,
  eventType: String,
  quantity: Int,
  timestamp: String
)

object Day30BatchApp {

  def main(args: Array[String]): Unit = {

    val conf =
      new SparkConf()
        .setAppName("Day30-Ecommerce-Batch")
        .setMaster("local[4]")

    val spark =
      SparkSession.builder()
        .config(conf)
        .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    println()
    println("================================================")
    println("       DAY 30 - E-COMMERCE BATCH")
    println("================================================")

    // ------------------------------------------------
    // 1. RAW RDD
    // ------------------------------------------------

    val rawEvents =
      sc.textFile("data/events.csv")

    println()
    println("RAW EVENTS")
    println("----------")
    println(s"Raw records = ${rawEvents.count()}")

    // ------------------------------------------------
    // 2. ACCUMULATOR
    // ------------------------------------------------

    val invalidEventAccumulator =
      sc.longAccumulator("InvalidEventCounter")

    // ------------------------------------------------
    // 3. CLEAN DATASET
    // ------------------------------------------------

    val cleanEvents: Dataset[EcommerceEvent] =
      rawEvents.flatMap { line =>

        val parts =
          line.split(",")

        try {

          if (parts.length != 6) {

            invalidEventAccumulator.add(1)

            None

          } else {

            Some(
              EcommerceEvent(
                parts(0).trim,
                parts(1).trim,
                parts(2).trim,
                parts(3).trim.toUpperCase,
                parts(4).trim.toInt,
                parts(5).trim
              )
            )
          }

        } catch {

          case _: Exception =>

            invalidEventAccumulator.add(1)

            None
        }

      }.toDS()

    // ------------------------------------------------
    // 4. CACHE / PERSIST
    // ------------------------------------------------

    val cachedEvents =
      cleanEvents.persist(
        StorageLevel.MEMORY_AND_DISK
      )

    println()
    println("CLEAN DATA")
    println("----------")

    cachedEvents.show(
      20,
      truncate = false
    )

    println(
      s"Invalid events = ${invalidEventAccumulator.value}"
    )

    // ------------------------------------------------
    // 5. PAIR RDD
    // ------------------------------------------------

    val eventTypeCounts =
      cachedEvents.rdd
        .map(event =>
          (
            event.eventType,
            1
          )
        )
        .reduceByKey(_ + _)

    println()
    println("EVENT TYPE COUNTS")
    println("-----------------")

    eventTypeCounts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (eventType, count) =>
          println(
            s"$eventType -> $count"
          )
      }

    // ------------------------------------------------
    // 6. PARTITION TUNING
    // ------------------------------------------------

    val partitionedEvents =
      cachedEvents.rdd
        .map(event =>
          (
            event.productId,
            event.quantity
          )
        )
        .partitionBy(
          new HashPartitioner(4)
        )

    println()
    println("PARTITION TUNING")
    println("----------------")

    println(
      s"Partitions after tuning = " +
      s"${partitionedEvents.getNumPartitions}"
    )

    // ------------------------------------------------
    // 7. PRODUCT DATAFRAME
    // ------------------------------------------------

    val productSchema =
      StructType(
        Seq(
          StructField(
            "productId",
            StringType,
            false
          ),
          StructField(
            "productName",
            StringType,
            false
          ),
          StructField(
            "category",
            StringType,
            false
          ),
          StructField(
            "price",
            DoubleType,
            false
          )
        )
      )

    val productsDF =
      spark.read
        .schema(productSchema)
        .option("header", "false")
        .csv("data/products.csv")

    // ------------------------------------------------
    // 8. CUSTOMER DATAFRAME
    // ------------------------------------------------

    val customerSchema =
      StructType(
        Seq(
          StructField(
            "customerId",
            StringType,
            false
          ),
          StructField(
            "city",
            StringType,
            false
          ),
          StructField(
            "segment",
            StringType,
            false
          )
        )
      )

    val customersDF =
      spark.read
        .schema(customerSchema)
        .option("header", "false")
        .csv("data/customers.csv")

    // ------------------------------------------------
    // 9. BROADCAST
    // ------------------------------------------------

    val broadcastProducts =
      broadcast(productsDF)

    val broadcastCustomers =
      broadcast(customersDF)

    // ------------------------------------------------
    // 10. JOINS
    // ------------------------------------------------

    val enriched =
      cachedEvents
        .toDF()
        .join(
          broadcastProducts,
          Seq("productId"),
          "left"
        )
        .join(
          broadcastCustomers,
          Seq("customerId"),
          "left"
        )

    // ------------------------------------------------
    // 11. UDF
    // ------------------------------------------------

    val customerValueUDF =
      udf {
        segment: String =>

          segment match {

            case "PREMIUM" =>
              "HIGH_VALUE"

            case "REGULAR" =>
              "STANDARD_VALUE"

            case _ =>
              "UNKNOWN"
          }
      }

    // ------------------------------------------------
    // 12. REVENUE
    // ------------------------------------------------

    val enrichedFinal =
      enriched
        .withColumn(
          "customer_value",
          customerValueUDF(
            col("segment")
          )
        )
        .withColumn(
          "revenue",
          when(
            col("eventType") === "PURCHASE",
            col("quantity") * col("price")
          ).otherwise(
            lit(0.0)
          )
        )
        .withColumn(
          "event_timestamp",
          to_timestamp(
            col("timestamp")
          )
        )
        .persist(
          StorageLevel.MEMORY_AND_DISK
        )

    println()
    println("ENRICHED DATA")
    println("-------------")

    enrichedFinal.show(
      20,
      truncate = false
    )

    // ------------------------------------------------
    // 13. WINDOW FUNCTION
    // ------------------------------------------------

    val customerWindow =
      Window
        .partitionBy("customerId")
        .orderBy("event_timestamp")
        .rowsBetween(
          Window.unboundedPreceding,
          Window.currentRow
        )

    val runningRevenue =
      enrichedFinal
        .withColumn(
          "running_revenue",
          sum("revenue")
            .over(customerWindow)
        )

    println()
    println("CUSTOMER RUNNING REVENUE")
    println("------------------------")

    runningRevenue
      .select(
        "customerId",
        "eventId",
        "eventType",
        "revenue",
        "running_revenue"
      )
      .orderBy(
        "customerId",
        "event_timestamp"
      )
      .show(
        30,
        truncate = false
      )

    // ------------------------------------------------
    // 14. PRODUCT AGGREGATION
    // ------------------------------------------------

    val productSales =
      enrichedFinal
        .filter(
          col("eventType") === "PURCHASE"
        )
        .groupBy(
          "productId",
          "productName",
          "category"
        )
        .agg(
          sum("quantity")
            .alias("units_sold"),

          sum("revenue")
            .alias("total_revenue")
        )
        .orderBy(
          desc("total_revenue")
        )

    println()
    println("PRODUCT SALES")
    println("-------------")

    productSales.show(
      truncate = false
    )

    // ------------------------------------------------
    // 15. CUSTOMER AGGREGATION
    // ------------------------------------------------

    val customerSales =
      enrichedFinal
        .filter(
          col("eventType") === "PURCHASE"
        )
        .groupBy(
          "customerId",
          "city",
          "segment"
        )
        .agg(
          sum("quantity")
            .alias("items_purchased"),

          sum("revenue")
            .alias("customer_revenue")
        )
        .orderBy(
          desc("customer_revenue")
        )

    println()
    println("CUSTOMER SALES")
    println("--------------")

    customerSales.show(
      truncate = false
    )

    // ------------------------------------------------
    // 16. CATEGORY AGGREGATION
    // ------------------------------------------------

    val categorySales =
      enrichedFinal
        .filter(
          col("eventType") === "PURCHASE"
        )
        .groupBy(
          "category"
        )
        .agg(
          sum("quantity")
            .alias("units_sold"),

          sum("revenue")
            .alias("revenue")
        )
        .orderBy(
          desc("revenue")
        )

    println()
    println("CATEGORY SALES")
    println("--------------")

    categorySales.show(
      truncate = false
    )

    // ------------------------------------------------
    // 17. SPARK SQL
    // ------------------------------------------------

    enrichedFinal.createOrReplaceTempView(
      "ecommerce_events"
    )

    val sqlReport =
      spark.sql(
        """
        SELECT
            category,
            COUNT(*) AS purchase_events,
            SUM(quantity) AS units_sold,
            ROUND(SUM(revenue), 2) AS revenue
        FROM ecommerce_events
        WHERE eventType = 'PURCHASE'
        GROUP BY category
        ORDER BY revenue DESC
        """
      )

    println()
    println("SPARK SQL REPORT")
    println("----------------")

    sqlReport.show(
      truncate = false
    )

    // ------------------------------------------------
    // 18. QUERY PLAN / DAG
    // ------------------------------------------------

    println()
    println("EXECUTION PLAN")
    println("--------------")

    productSales.explain(true)

    // ------------------------------------------------
    // 19. FINAL SUMMARY
    // ------------------------------------------------

    val totalEvents =
      cachedEvents.count()

    val purchaseEvents =
      enrichedFinal
        .filter(
          col("eventType") === "PURCHASE"
        )
        .count()

    val totalRevenue =
      enrichedFinal
        .agg(
          sum("revenue")
            .alias("total_revenue")
        )
        .collect()
        .head
        .getAs[Double](
          "total_revenue"
        )

    println()
    println("================================================")
    println("          FINAL BATCH SUMMARY")
    println("================================================")

    println(
      s"Total events       = $totalEvents"
    )

    println(
      s"Purchase events    = $purchaseEvents"
    )

    println(
      f"Total revenue      = ₹$totalRevenue%.2f"
    )

    println(
      s"Invalid events     = " +
      s"${invalidEventAccumulator.value}"
    )

    println(
      s"Partition count    = " +
      s"${partitionedEvents.getNumPartitions}"
    )

    println("================================================")

    enrichedFinal.unpersist()
    cachedEvents.unpersist()

    spark.stop()
  }
}
