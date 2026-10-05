import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

case class BookingEvent(
  eventId: String,
  resourceId: String,
  resourceType: String,
  action: String,
  userId: String,
  quantity: Int,
  timestamp: String
)

case class ResourceInfo(
  resourceId: String,
  resourceType: String,
  name: String,
  capacity: Int,
  location: String
)

object Day28BookingApp {

  def main(args: Array[String]): Unit = {

    // ==================================================
    // SPARK CONFIGURATION
    // ==================================================

    val conf = new SparkConf()
      .setAppName("Day28-RealTimeBooking")
      .setMaster("local[4]")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.checkpoint("checkpoint")

    val spark =
      SparkSession.builder()
        .config(conf)
        .getOrCreate()

    // ==================================================
    // APPLICATION HEADER
    // ==================================================

    println()
    println("================================================")
    println("        DAY 28 - REAL-TIME BOOKING")
    println("================================================")
    println()
    println("Batch interval : 5 seconds")
    println("Window size    : 30 seconds")
    println("Slide interval : 5 seconds")
    println("Socket         : localhost:9999")
    println()

    // ==================================================
    // BROADCAST REFERENCE DATA
    // ==================================================

    val resourceReference =
      Map(
        "HOTEL101" ->
          ResourceInfo(
            "HOTEL101",
            "HOTEL",
            "Taj Hyderabad",
            100,
            "HYDERABAD"
          ),

        "HOTEL102" ->
          ResourceInfo(
            "HOTEL102",
            "HOTEL",
            "ITC Delhi",
            80,
            "DELHI"
          ),

        "FLIGHT201" ->
          ResourceInfo(
            "FLIGHT201",
            "FLIGHT",
            "AI-201",
            180,
            "DELHI-MUMBAI"
          ),

        "FLIGHT202" ->
          ResourceInfo(
            "FLIGHT202",
            "FLIGHT",
            "6E-202",
            150,
            "HYDERABAD-DELHI"
          ),

        "BUS301" ->
          ResourceInfo(
            "BUS301",
            "BUS",
            "TSRTC-301",
            45,
            "HYDERABAD-BANGALORE"
          ),

        "BUS302" ->
          ResourceInfo(
            "BUS302",
            "BUS",
            "KSRTC-302",
            40,
            "BANGALORE-CHENNAI"
          )
      )

    val broadcastResources:
      Broadcast[Map[String, ResourceInfo]] =
      streamingContext.sparkContext.broadcast(
        resourceReference
      )

    // ==================================================
    // SOCKET STREAM
    // ==================================================

    val inputStream =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )

    // ==================================================
    // PARSE BOOKING EVENTS
    // ==================================================

    val bookingEvents =
      inputStream.flatMap { line =>

        val parts = line.split(",")

        if (parts.length == 7) {

          try {

            Some(
              BookingEvent(
                parts(0).trim,
                parts(1).trim,
                parts(2).trim,
                parts(3).trim.toUpperCase,
                parts(4).trim,
                parts(5).trim.toInt,
                parts(6).trim
              )
            )

          } catch {

            case _: Exception =>
              None
          }

        } else {

          None
        }
      }

    // ==================================================
    // CACHE / PERSIST
    // ==================================================

    val cachedEvents =
      bookingEvents.persist(
        StorageLevel.MEMORY_ONLY
      )

    // ==================================================
    // 1. PAIR RDD
    // CURRENT BOOKING ACTIVITY
    // ==================================================

    val resourceBookingCounts =
      cachedEvents
        .map { event =>

          val signedQuantity =
            if (event.action == "BOOK") {
              event.quantity
            } else {
              -event.quantity
            }

          (
            event.resourceId,
            signedQuantity
          )
        }
        .reduceByKey(_ + _)

    resourceBookingCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("          CURRENT BOOKING ACTIVITY")
        println("----------------------------------------------")

        rdd.collect()
          .sortBy(_._1)
          .foreach {

            case (resourceId, quantity) =>

              println(
                s"Resource=$resourceId | Net Change=$quantity"
              )
          }
      }
    }

    // ==================================================
    // 2. STATEFUL BOOKING / CANCELLATION STATE
    // ==================================================

    val stateChanges =
      cachedEvents.map { event =>

        val signedQuantity =
          if (event.action == "BOOK") {
            event.quantity
          } else {
            -event.quantity
          }

        (
          event.resourceId,
          signedQuantity
        )
      }

    val runningBookingState =
      stateChanges.updateStateByKey[Int] {

        (
          newValues: Seq[Int],
          previousState: Option[Int]
        ) => {

          val previous =
            previousState.getOrElse(0)

          val currentChange =
            newValues.sum

          Some(previous + currentChange)
        }
      }

    // ==================================================
    // 3. OCCUPANCY AND AVAILABILITY
    // ==================================================

    runningBookingState.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("       OCCUPANCY & AVAILABILITY")
        println("----------------------------------------------")

        rdd.collect()
          .sortBy(_._1)
          .foreach {

            case (resourceId, booked) =>

              broadcastResources.value
                .get(resourceId)
                .foreach { info =>

                  val capacity =
                    info.capacity

                  val safeBooked =
                    math.max(
                      0,
                      math.min(
                        booked,
                        capacity
                      )
                    )

                  val availability =
                    math.max(
                      0,
                      capacity - safeBooked
                    )

                  val occupancy =
                    if (capacity > 0) {

                      safeBooked.toDouble /
                        capacity.toDouble * 100

                    } else {

                      0.0
                    }

                  println(
                    f"Resource=$resourceId%-10s | " +
                    f"Type=${info.resourceType}%-6s | " +
                    f"Booked=$safeBooked%-4d | " +
                    f"Available=$availability%-4d | " +
                    f"Occupancy=$occupancy%.2f%%"
                  )
                }
          }
      }
    }

    // ==================================================
    // 4. 30-SECOND BOOKING WINDOW
    // ==================================================

    val bookingWindow =
      cachedEvents
        .filter(_.action == "BOOK")
        .map { event =>

          (
            event.resourceId,
            event.quantity
          )
        }
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          Seconds(30),
          Seconds(5)
        )

    bookingWindow.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("          30-SECOND BOOKING WINDOW")
        println("----------------------------------------------")

        rdd.collect()
          .sortBy(_._1)
          .foreach {

            case (resourceId, bookings) =>

              println(
                s"Resource=$resourceId | " +
                s"Bookings in last 30 seconds=$bookings"
              )
          }
      }
    }

    // ==================================================
    // 5. HIGH-DEMAND RESOURCE DETECTION
    // ==================================================

    val highDemand =
      bookingWindow.filter {

        case (_, bookings) =>
          bookings >= 3
      }

    highDemand.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==============================================")
        println("          HIGH-DEMAND ALERT")
        println("==============================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {

            case (resourceId, bookings) =>

              println(
                s"ALERT | Resource=$resourceId | " +
                s"$bookings bookings in last 30 seconds"
              )
          }
      }
    }

    // ==================================================
    // 6. SPARK SQL REPORTING
    // ==================================================

    runningBookingState.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val rows =
          rdd.collect().flatMap {

            case (resourceId, booked) =>

              broadcastResources.value
                .get(resourceId)
                .map { info =>

                  val safeBooked =
                    math.max(
                      0,
                      math.min(
                        booked,
                        info.capacity
                      )
                    )

                  val available =
                    math.max(
                      0,
                      info.capacity - safeBooked
                    )

                  val occupancy =
                    if (info.capacity > 0) {

                      safeBooked.toDouble /
                        info.capacity.toDouble * 100

                    } else {

                      0.0
                    }

                  Row(
                    resourceId,
                    info.resourceType,
                    info.name,
                    info.location,
                    info.capacity,
                    safeBooked,
                    available,
                    occupancy
                  )
                }
          }

        if (rows.nonEmpty) {

          val schema =
            StructType(
              Seq(
                StructField(
                  "resource_id",
                  StringType,
                  nullable = false
                ),

                StructField(
                  "resource_type",
                  StringType,
                  nullable = false
                ),

                StructField(
                  "resource_name",
                  StringType,
                  nullable = false
                ),

                StructField(
                  "location",
                  StringType,
                  nullable = false
                ),

                StructField(
                  "capacity",
                  IntegerType,
                  nullable = false
                ),

                StructField(
                  "booked",
                  IntegerType,
                  nullable = false
                ),

                StructField(
                  "available",
                  IntegerType,
                  nullable = false
                ),

                StructField(
                  "occupancy_percent",
                  DoubleType,
                  nullable = false
                )
              )
            )

          val reportDF =
            spark.createDataFrame(
              spark.sparkContext.parallelize(rows),
              schema
            )

          reportDF.createOrReplaceTempView(
            "booking_report"
          )

          println()
          println("----------------------------------------------")
          println("             SPARK SQL REPORT")
          println("----------------------------------------------")

          spark.sql(
            """
              SELECT
                resource_id,
                resource_type,
                resource_name,
                capacity,
                booked,
                available,
                ROUND(occupancy_percent, 2)
                  AS occupancy_percent
              FROM booking_report
              ORDER BY occupancy_percent DESC
            """
          ).show(false)
        }
      }
    }

    // ==================================================
    // START STREAMING
    // ==================================================

    println("----------------------------------------------")
    println("Streaming application started.")
    println("Waiting for booking events...")
    println("----------------------------------------------")
    println()

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
