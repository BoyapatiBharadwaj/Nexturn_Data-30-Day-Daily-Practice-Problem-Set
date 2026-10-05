import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

case class StreamEvent(
  eventId: String,
  customerId: String,
  productId: String,
  eventType: String,
  quantity: Int,
  timestamp: String
)

object Day30StreamingApp {

  def main(args: Array[String]): Unit = {

    val conf =
      new SparkConf()
        .setAppName("Day30-Ecommerce-Streaming")
        .setMaster("local[4]")

    val ssc =
      new StreamingContext(
        conf,
        Seconds(5)
      )

    ssc.checkpoint("checkpoint")

    val spark =
      SparkSession.builder()
        .config(ssc.sparkContext.getConf)
        .getOrCreate()

    import spark.implicits._

    println()
    println("================================================")
    println("      DAY 30 - E-COMMERCE STREAMING")
    println("================================================")
    println()
    println("Listening on localhost:9999")
    println()

    // ------------------------------------------------
    // REFERENCE DATA
    // ------------------------------------------------

    val products =
      Map(
        "P001" -> ("Laptop", "Electronics", 55000.0),
        "P002" -> ("Smartphone", "Electronics", 30000.0),
        "P003" -> ("Headphones", "Electronics", 2500.0),
        "P004" -> ("Keyboard", "Accessories", 1800.0),
        "P005" -> ("Mouse", "Accessories", 900.0),
        "P006" -> ("Monitor", "Electronics", 15000.0),
        "P007" -> ("Backpack", "Travel", 2200.0),
        "P008" -> ("Shoes", "Fashion", 3500.0)
      )

    val customers =
      Map(
        "C001" -> ("Hyderabad", "PREMIUM"),
        "C002" -> ("Delhi", "REGULAR"),
        "C003" -> ("Mumbai", "PREMIUM"),
        "C004" -> ("Bangalore", "REGULAR"),
        "C005" -> ("Chennai", "PREMIUM"),
        "C006" -> ("Pune", "REGULAR")
      )

    val productBroadcast =
      ssc.sparkContext.broadcast(products)

    val customerBroadcast =
      ssc.sparkContext.broadcast(customers)

    // ------------------------------------------------
    // ACCUMULATOR
    // ------------------------------------------------

    val invalidEvents =
      ssc.sparkContext.longAccumulator(
        "StreamingInvalidEvents"
      )

    // ------------------------------------------------
    // SOCKET SOURCE
    // ------------------------------------------------

    val input =
      ssc.socketTextStream(
        "localhost",
        9999
      )

    // ------------------------------------------------
    // PARSE
    // ------------------------------------------------

    val parsed =
      input.flatMap { line =>

        val parts =
          line.split(",")

        try {

          if (parts.length != 6) {

            invalidEvents.add(1)

            None

          } else {

            Some(
              StreamEvent(
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

            invalidEvents.add(1)

            None
        }
      }

    val cachedStream =
      parsed.persist(
        StorageLevel.MEMORY_ONLY
      )

    // ------------------------------------------------
    // CURRENT BATCH
    // ------------------------------------------------

    cachedStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val events =
          rdd.collect()

        println()
        println("----------------------------------------------")
        println("NEW STREAMING BATCH")
        println("----------------------------------------------")

        events.foreach { event =>

          println(
            s"${event.eventId} | " +
            s"${event.customerId} | " +
            s"${event.productId} | " +
            s"${event.eventType} | " +
            s"${event.quantity}"
          )
        }

        println(
          s"Current batch events = ${rdd.count()}"
        )

        println(
          s"Invalid events = ${invalidEvents.value}"
        )
      }
    }

    // ------------------------------------------------
    // STREAMING PRODUCT QUANTITY
    // ------------------------------------------------

    val productQuantity =
      cachedStream
        .map(event =>
          (
            event.productId,
            event.quantity
          )
        )
        .reduceByKey(
          (a: Int, b: Int) => a + b
        )

    productQuantity.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("CURRENT PRODUCT ACTIVITY")
        println("-----------------------")

        rdd
          .collect()
          .sortBy(_._1)
          .foreach {
            case (product, quantity) =>
              println(
                s"$product -> $quantity units"
              )
          }
      }
    }

    // ------------------------------------------------
    // 30 SECOND WINDOW
    // ------------------------------------------------

    val windowedPurchases =
      cachedStream
        .filter(
          _.eventType == "PURCHASE"
        )
        .map(event =>
          (
            event.productId,
            event.quantity
          )
        )
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          Seconds(30),
          Seconds(10)
        )

    // ------------------------------------------------
    // HIGH DEMAND ALERT
    // ------------------------------------------------

    windowedPurchases.foreachRDD { rdd =>

      val alerts =
        rdd.filter {
          case (_, quantity) =>
            quantity >= 3
        }

      if (!alerts.isEmpty()) {

        println()
        println("!!! HIGH DEMAND ALERT !!!")
        println("-------------------------")

        alerts
          .collect()
          .foreach {
            case (productId, quantity) =>
              println(
                s"$productId -> " +
                s"$quantity purchases " +
                s"in last 30 seconds"
              )
          }
      }
    }

    // ------------------------------------------------
    // STREAMING SQL
    // ------------------------------------------------

    cachedStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val rows =
          rdd.map { event =>

            val product =
              productBroadcast.value
                .get(event.productId)

            val customer =
              customerBroadcast.value
                .get(event.customerId)

            val productName =
              product.map(_._1)
                .getOrElse("UNKNOWN")

            val category =
              product.map(_._2)
                .getOrElse("UNKNOWN")

            val price =
              product.map(_._3)
                .getOrElse(0.0)

            val city =
              customer.map(_._1)
                .getOrElse("UNKNOWN")

            val segment =
              customer.map(_._2)
                .getOrElse("UNKNOWN")

            val revenue =
              if (event.eventType == "PURCHASE")
                event.quantity * price
              else
                0.0

            (
              event.eventId,
              event.customerId,
              event.productId,
              event.eventType,
              event.quantity,
              productName,
              category,
              city,
              segment,
              revenue
            )
          }
          .toDF(
            "eventId",
            "customerId",
            "productId",
            "eventType",
            "quantity",
            "productName",
            "category",
            "city",
            "segment",
            "revenue"
          )

        rows.createOrReplaceTempView(
          "streaming_events"
        )

        val report =
          spark.sql(
            """
            SELECT
                category,
                COUNT(*) AS events,
                SUM(quantity) AS units,
                ROUND(SUM(revenue), 2) AS revenue
            FROM streaming_events
            WHERE eventType = 'PURCHASE'
            GROUP BY category
            ORDER BY revenue DESC
            """
          )

        println()
        println("STREAMING SQL REPORT")
        println("-------------------")

        report.show(
          truncate = false
        )
      }
    }

    ssc.start()

    ssc.awaitTermination()
  }
}
