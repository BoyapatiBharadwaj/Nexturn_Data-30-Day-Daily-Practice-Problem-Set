import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreamsApp {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // 1. Create Spark configuration
    // ------------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day23 DStreams Log Monitoring")
      .setMaster("local[2]")

    // ------------------------------------------------------------
    // 2. Create StreamingContext
    // ------------------------------------------------------------

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("WARN")

    println("\n==============================================")
    println("DAY 23 - DSTREAMS LOG MONITORING")
    println("==============================================")

    println("Batch interval: 5 seconds")
    println("Listening on localhost:9999")

    // ------------------------------------------------------------
    // 3. Create socket stream
    // ------------------------------------------------------------

    val logStream =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )

    // ------------------------------------------------------------
    // 4. Display every incoming log line
    // ------------------------------------------------------------

    logStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== RAW BATCH ==========")

        rdd.collect().foreach { line =>
          println(line)
        }

        println(
          s"Records in current batch: ${rdd.count()}"
        )
      }
    }

    // ------------------------------------------------------------
    // 5. flatMap - extract words from log messages
    // ------------------------------------------------------------

    val words = logStream.flatMap { line =>
      line.split("\\s+")
    }

    // ------------------------------------------------------------
    // 6. filter - keep ERROR messages
    // ------------------------------------------------------------

    val errorLogs = logStream.filter { line =>
      line.toUpperCase.contains("ERROR")
    }

    // ------------------------------------------------------------
    // 7. Count ERROR messages in each batch
    // ------------------------------------------------------------

    val errorCount = errorLogs
      .map(_ => 1)
      .reduce(_ + _)

    // ------------------------------------------------------------
    // 8. Print ERROR count
    // ------------------------------------------------------------

    errorCount.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val count = rdd.first()

        println(
          s"\n******** ERROR COUNT IN CURRENT 5-SECOND BATCH: $count ********"
        )
      } else {

        println(
          "\n******** ERROR COUNT IN CURRENT 5-SECOND BATCH: 0 ********"
        )
      }
    }

    // ------------------------------------------------------------
    // 9. Count log levels
    // ------------------------------------------------------------

    val logLevels = logStream.map { line =>

      val fields = line.trim.split("\\s+", 4)

      if (fields.length >= 3) {
        fields(2).toUpperCase
      } else {
        "UNKNOWN"
      }
    }

    val levelCounts = logLevels
      .map(level => (level, 1))
      .reduceByKey(_ + _)

    levelCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== LOG LEVEL COUNTS ==========")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (level, count) =>
              println(s"$level -> $count")
          }
      }
    }

    // ------------------------------------------------------------
    // 10. Start streaming
    // ------------------------------------------------------------

    streamingContext.start()

    println("\nStreaming application started.")
    println("Waiting for log messages...")
    println("Press Ctrl+C to stop.")

    // ------------------------------------------------------------
    // 11. Wait for streaming termination
    // ------------------------------------------------------------

    streamingContext.awaitTermination()
  }
}
