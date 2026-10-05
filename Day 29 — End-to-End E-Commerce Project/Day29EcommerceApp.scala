import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

// ======================================================
// CASE CLASSES
// ======================================================

case class EcommerceEvent(
  eventId: String,
  customerId: String,
  productId: String,
  eventType: String,
  quantity: Int,
  timestamp: String
)

case class Product(
  productId: String,
  productName: String,
  category: String,
  price: Double
)

case class Customer(
  customerId: String,
  city: String,
  segment: String
)

// ======================================================
// MAIN APPLICATION
// ======================================================

object Day29EcommerceApp {

  def main(args: Array[String]): Unit = {

    // ==================================================
    // SPARK CONFIGURATION
    // ==================================================

    val conf =
      new SparkConf()
        .setAppName("Day29-EndToEndEcommerce")
        .setMaster("local[4]")

    val spark =
      SparkSession.builder()
        .config(conf)
        .getOrCreate()

    val sc =
      spark.sparkContext

    import spark.implicits._

    // ==================================================
    // HEADER
    // ==================================================

    println()
    println("================================================")
    println("       DAY 29 - END-TO-END E-COMMERCE")
    println("================================================")
    println()
    println("Pipeline:")
    println("RAW -> CLEAN -> ENRICH -> AGGREGATE")
    println()

    // ==================================================
    // 1. RAW LAYER - RDD
    // ==================================================

    println("----------------------------------------------")
    println("              RAW DATA LAYER")
    println("----------------------------------------------")

    val rawEvents =
      sc.textFile("data/events.csv")

    println(
      s"Raw event records = ${rawEvents.count()}"
    )

    // Remove header and malformed records
    val rawEventRDD =
      rawEvents
        .filter(line => !line.startsWith("eventId"))
        .filter(line => line.split(",").length == 6)

    // ==================================================
    // 2. PAIR RDD
    // ==================================================

    val eventTypeCounts =
      rawEventRDD
        .map { line =>

          val parts =
            line.split(",")

          (
            parts(3).trim.toUpperCase,
            1
          )
        }
        .reduceByKey(_ + _)

    println()
    println("----------------------------------------------")
    println("          EVENT TYPE COUNTS")
    println("----------------------------------------------")

    eventTypeCounts
      .collect()
      .sortBy(_._1)
      .foreach {

        case (eventType, count) =>

          println(
            s"$eventType -> $count"
          )
      }

    // ==================================================
    // 3. CLEAN LAYER - DATASET
    // ==================================================

    val eventsDS: Dataset[EcommerceEvent] =
      rawEventRDD
        .flatMap { line =>

          val parts =
            line.split(",")

          try {

            Some(
              EcommerceEvent(
                eventId = parts(0).trim,
                customerId = parts(1).trim,
                productId = parts(2).trim,
                eventType = parts(3).trim.toUpperCase,
                quantity = parts(4).trim.toInt,
                timestamp = parts(5).trim
              )
            )

          } catch {

            case _: Exception =>
              None
          }
        }
        .toDS()

    // ==================================================
    // 4. PERSIST CLEAN DATA
    // ==================================================

    val cachedEvents =
      eventsDS.persist(
        StorageLevel.MEMORY_ONLY
      )

    println()
    println("----------------------------------------------")
    println("             CLEAN DATA")
    println("----------------------------------------------")

    cachedEvents.show(
      numRows = 15,
      truncate = false
    )

    // ==================================================
    // 5. PRODUCT REFERENCE DATA
    // ==================================================

    val productSchema =
      StructType(
        Seq(
          StructField(
            "productId",
            StringType,
            nullable = false
          ),
          StructField(
            "productName",
            StringType,
            nullable = false
          ),
          StructField(
            "category",
            StringType,
            nullable = false
          ),
          StructField(
            "price",
            DoubleType,
            nullable = false
          )
        )
      )

    val productsDF =
      spark.read
        .option("header", "false")
        .schema(productSchema)
        .csv("data/products.csv")

    // ==================================================
    // 6. CUSTOMER REFERENCE DATA
    // ==================================================

    val customerSchema =
      StructType(
        Seq(
          StructField(
            "customerId",
            StringType,
            nullable = false
          ),
          StructField(
            "city",
            StringType,
            nullable = false
          ),
          StructField(
            "segment",
            StringType,
            nullable = false
          )
        )
      )

    val customersDF =
      spark.read
        .option("header", "false")
        .schema(customerSchema)
        .csv("data/customers.csv")

    // ==================================================
    // 7. BROADCAST REFERENCE DATA
    // ==================================================

    val productsBroadcast =
      broadcast(productsDF)

    val customersBroadcast =
      broadcast(customersDF)

    // ==================================================
    // 8. ENRICHMENT - BROADCAST JOINS
    // ==================================================

    val enrichedEvents =
      cachedEvents
        .toDF()
        .join(
          productsBroadcast,
          Seq("productId"),
          "left"
        )
        .join(
          customersBroadcast,
          Seq("customerId"),
          "left"
        )

    // ==================================================
    // 9. UDF
    // ==================================================

    val customerValue =
      udf { segment: String =>

        segment match {

          case "PREMIUM" =>
            "HIGH_VALUE"

          case "REGULAR" =>
            "STANDARD_VALUE"

          case _ =>
            "UNKNOWN"
        }
      }

    val enrichedWithValue =
      enrichedEvents
        .withColumn(
          "customer_value",
          customerValue(
            col("segment")
          )
        )

    // ==================================================
    // 10. REVENUE CALCULATION
    // ==================================================

    val enrichedFinal =
      enrichedWithValue
        .withColumn(
          "revenue",
          when(
            col("eventType") === "PURCHASE",
            col("quantity") * col("price")
          )
            .otherwise(
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
    println("----------------------------------------------")
    println("             ENRICHED DATA")
    println("----------------------------------------------")

    enrichedFinal.show(
      numRows = 20,
      truncate = false
    )

    // ==================================================
    // 11. WINDOW FUNCTION
    // ==================================================

    val customerWindow =
      Window
        .partitionBy(
          "customerId"
        )
        .orderBy(
          col("event_timestamp")
        )

    val customerRunningRevenue =
      enrichedFinal
        .withColumn(
          "running_revenue",
          sum("revenue")
            .over(customerWindow)
        )

    println()
    println("----------------------------------------------")
    println("        CUSTOMER RUNNING REVENUE")
    println("----------------------------------------------")

    customerRunningRevenue
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

    // ==================================================
    // 12. PRODUCT AGGREGATION
    // ==================================================

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
    println("----------------------------------------------")
    println("            PRODUCT SALES")
    println("----------------------------------------------")

    productSales.show(
      truncate = false
    )

    // ==================================================
    // 13. CUSTOMER AGGREGATION
    // ==================================================

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
    println("----------------------------------------------")
    println("            CUSTOMER SALES")
    println("----------------------------------------------")

    customerSales.show(
      truncate = false
    )

    // ==================================================
    // 14. CATEGORY AGGREGATION
    // ==================================================

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
    println("----------------------------------------------")
    println("            CATEGORY SALES")
    println("----------------------------------------------")

    categorySales.show(
      truncate = false
    )

    // ==================================================
    // 15. SPARK SQL REPORTING
    // ==================================================

    enrichedFinal.createOrReplaceTempView(
      "ecommerce_events"
    )

    println()
    println("----------------------------------------------")
    println("             SQL REPORT")
    println("----------------------------------------------")

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

    sqlReport.show(
      truncate = false
    )

    // ==================================================
    // 16. PARTITION INFORMATION
    // ==================================================

    println()
    println("----------------------------------------------")
    println("          PARTITION INFORMATION")
    println("----------------------------------------------")

    println(
      s"Raw RDD partitions = " +
      s"${rawEvents.getNumPartitions}"
    )

    println(
      s"Clean Dataset partitions = " +
      s"${cachedEvents.rdd.getNumPartitions}"
    )

    println(
      s"Enriched DataFrame partitions = " +
      s"${enrichedFinal.rdd.getNumPartitions}"
    )

    // ==================================================
    // 17. FINAL SUMMARY
    // ==================================================

    val totalEvents =
      cachedEvents.count()

    val purchaseEvents =
      enrichedFinal
        .filter(
          col("eventType") === "PURCHASE"
        )
        .count()

    val totalRevenueRow =
      enrichedFinal
        .agg(
          sum("revenue")
            .alias("total_revenue")
        )
        .collect()
        .head

    val totalRevenue =
      Option(
        totalRevenueRow.getAs[Double](
          "total_revenue"
        )
      ).getOrElse(0.0)

    println()
    println("================================================")
    println("             FINAL E-COMMERCE SUMMARY")
    println("================================================")

    println(
      s"Total events      = $totalEvents"
    )

    println(
      s"Purchase events   = $purchaseEvents"
    )

    println(
      f"Total revenue     = ₹$totalRevenue%.2f"
    )

    println()
    println(
      "RAW -> CLEAN -> ENRICH -> AGGREGATE completed."
    )

    println("================================================")

    // ==================================================
    // CLEANUP
    // ==================================================

    enrichedFinal.unpersist()
    cachedEvents.unpersist()

    spark.stop()
  }
}
