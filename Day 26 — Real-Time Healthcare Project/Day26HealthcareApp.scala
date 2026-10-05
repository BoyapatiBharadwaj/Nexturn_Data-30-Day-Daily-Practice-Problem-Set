import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

case class PatientVital(
    patientId: String,
    vitalType: String,
    value: Double,
    timestamp: String
)

case class VitalThreshold(
    min: Double,
    max: Double
)

object Day26HealthcareApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // SPARK CONFIGURATION
    // ============================================================

    val conf = new SparkConf()
      .setAppName("Day26 Healthcare Monitoring")
      .setMaster("local[4]")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    // Required for window processing
    streamingContext.checkpoint("checkpoint")

    // ============================================================
    // APPLICATION STARTUP MESSAGE
    // ============================================================

    println()
    println("==============================================")
    println("       DAY 26 - HEALTHCARE MONITORING")
    println("==============================================")
    println()
    println("Batch interval : 5 seconds")
    println("Window size    : 30 seconds")
    println("Slide interval : 5 seconds")
    println("Socket         : localhost:9999")
    println()
    println("Streaming application started.")
    println("Waiting for patient vital events...")
    println()

    // ============================================================
    // 1. BROADCAST VITAL THRESHOLDS
    // ============================================================

    val thresholds = Map(
      "HEART_RATE" -> VitalThreshold(60.0, 100.0),
      "OXYGEN" -> VitalThreshold(95.0, 100.0),
      "TEMPERATURE" -> VitalThreshold(36.0, 38.0)
    )

    val broadcastThresholds: Broadcast[
      Map[String, VitalThreshold]
    ] =
      streamingContext.sparkContext.broadcast(thresholds)

    // ============================================================
    // 2. ACCUMULATOR
    // ============================================================

    val abnormalVitalAccumulator: LongAccumulator =
      streamingContext.sparkContext.longAccumulator(
        "Total Abnormal Vital Events"
      )

    // ============================================================
    // 3. SOCKET STREAM
    // ============================================================

    val rawStream =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )

    // ============================================================
    // 4. PARSE PATIENT VITAL EVENTS
    // ============================================================

    val vitals =
      rawStream.flatMap { line =>

        val fields = line.split(",")

        if (fields.length == 4) {

          try {

            Some(
              PatientVital(
                patientId = fields(0).trim,
                vitalType = fields(1).trim.toUpperCase,
                value = fields(2).trim.toDouble,
                timestamp = fields(3).trim
              )
            )

          } catch {

            case _: NumberFormatException =>
              None
          }

        } else {

          None
        }
      }

    // ============================================================
    // 5. DETECT ABNORMAL VITALS
    // ============================================================

    val abnormalVitals =
      vitals.filter { vital =>

        broadcastThresholds.value
          .get(vital.vitalType)
          .exists { threshold =>

            vital.value < threshold.min ||
            vital.value > threshold.max
          }
      }

    // ============================================================
    // 6. ABNORMAL VITAL ALERTS
    // ============================================================

    abnormalVitals.foreachRDD { rdd =>

      val abnormalCount = rdd.count()

      if (abnormalCount > 0) {

        // Update accumulator
        abnormalVitalAccumulator.add(abnormalCount)

        println()
        println("----------------------------------------------")
        println("           ABNORMAL VITAL ALERTS")
        println("----------------------------------------------")

        rdd.collect().foreach { vital =>

          val threshold =
            broadcastThresholds.value(vital.vitalType)

          println(
            f"ALERT | Patient=${vital.patientId}%-5s | " +
            f"Vital=${vital.vitalType}%-12s | " +
            f"Value=${vital.value}%.1f | " +
            f"Normal=${threshold.min}%.1f-${threshold.max}%.1f | " +
            f"Time=${vital.timestamp}"
          )
        }

        println()
        println(
          s"Total abnormal events so far = " +
            s"${abnormalVitalAccumulator.value}"
        )
      }
    }

    // ============================================================
    // 7. CURRENT BATCH ABNORMAL COUNT BY PATIENT
    // ============================================================

    val patientCurrentCounts =
      abnormalVitals
        .map(vital => (vital.patientId, 1))
        .reduceByKey(_ + _)

    patientCurrentCounts.foreachRDD { rdd =>

      val results = rdd.collect()

      if (results.nonEmpty) {

        println()
        println("----------------------------------------------")
        println("       CURRENT BATCH ABNORMAL COUNTS")
        println("----------------------------------------------")

        results
          .sortBy(_._1)
          .foreach {
            case (patientId, count) =>

              println(
                f"Patient=$patientId%-5s | " +
                f"Current abnormal readings=$count"
              )
          }
      }
    }

    // ============================================================
    // 8. 30-SECOND WINDOW
    // ============================================================

    val windowedAbnormalVitals =
      abnormalVitals
        .map(vital => (vital.patientId, 1))
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          Seconds(30),
          Seconds(5)
        )

    // ============================================================
    // 9. REPEATED ABNORMAL VITAL DETECTION
    // ============================================================

    val repeatedAbnormalPatients =
      windowedAbnormalVitals.filter {
        case (_, count) =>
          count >= 3
      }

    repeatedAbnormalPatients.foreachRDD { rdd =>

      val results = rdd.collect()

      if (results.nonEmpty) {

        println()
        println("==============================================")
        println("       CRITICAL REPEATED-VITAL ALERT")
        println("==============================================")

        results
          .sortBy(_._1)
          .foreach {
            case (patientId, count) =>

              println(
                s"CRITICAL | Patient=$patientId | " +
                  s"$count abnormal readings in last 30 seconds"
              )
          }
      }
    }

    // ============================================================
    // 10. TOTAL ABNORMAL VITALS IN 30-SECOND WINDOW
    // ============================================================

    val totalAbnormalWindow =
      abnormalVitals
        .map(_ => 1)
        .reduceByWindow(
          (a: Int, b: Int) => a + b,
          Seconds(30),
          Seconds(5)
        )

    totalAbnormalWindow.foreachRDD { rdd =>

      val results = rdd.collect()

      if (results.nonEmpty) {

        val total = results.head

        println()
        println("----------------------------------------------")
        println("             WINDOW SUMMARY")
        println("----------------------------------------------")
        println(
          s"30-second abnormal-vital window count = $total"
        )
      }
    }

    // ============================================================
    // START STREAMING
    // ============================================================

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
