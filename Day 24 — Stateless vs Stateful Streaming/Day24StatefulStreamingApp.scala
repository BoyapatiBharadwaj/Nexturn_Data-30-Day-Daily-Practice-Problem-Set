import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day24StatefulStreamingApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. Spark configuration
    // ============================================================

    val conf = new SparkConf()
      .setAppName("Day24 Stateful Streaming")
      .setMaster("local[2]")

    // ============================================================
    // 2. StreamingContext
    // ============================================================

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("WARN")

    // Required for stateful operations
    // in local learning environment.
    streamingContext.checkpoint("checkpoint")

    println("\n==============================================")
    println("DAY 24 - STATELESS VS STATEFUL STREAMING")
    println("==============================================")

    println("Batch interval : 5 seconds")
    println("Socket         : localhost:9999")
    println("Input format   : account_id,transaction_type,amount")

    // ============================================================
    // 3. Create socket stream
    // ============================================================

    val rawStream =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )

    // ============================================================
    // 4. Parse incoming transactions
    // ============================================================

    val transactions = rawStream.flatMap { line =>

      val fields = line.trim.split(",")

      if (fields.length == 3) {

        val accountId = fields(0).trim
        val transactionType = fields(1).trim.toUpperCase

        try {

          val amount = fields(2).trim.toDouble

          Some(
            (
              accountId,
              transactionType,
              amount
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
    // 5. STATELESS PROCESSING
    //
    // Count transactions only in the CURRENT batch.
    // ============================================================

    val currentBatchCounts =
      transactions
        .map {
          case (accountId, _, _) =>
            (accountId, 1)
        }
        .reduceByKey(_ + _)

    // ============================================================
    // 6. Display current-batch results
    // ============================================================

    currentBatchCounts.foreachRDD { rdd =>

      println("\n========== CURRENT BATCH COUNTS ==========")

      if (rdd.isEmpty()) {

        println("No transactions in this batch.")

      } else {

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (accountId, count) =>
              println(
                s"$accountId -> $count transaction(s) in current batch"
              )
          }
      }
    }

    // ============================================================
    // 7. STATEFUL PROCESSING
    //
    // updateFunction receives:
    //
    // currentValues = values from current batch
    // previousState = previously stored state
    //
    // It returns the new accumulated state.
    // ============================================================

    val updateFunction =
      (
        currentValues: Seq[Int],
        previousState: Option[Int]
      ) => {

        val currentCount =
          currentValues.sum

        val previousCount =
          previousState.getOrElse(0)

        Some(
          previousCount + currentCount
        )
      }

    // ============================================================
    // 8. Maintain running transaction counts
    // ============================================================

    val runningCounts: DStream[(String, Int)] =
      transactions
        .map {
          case (accountId, _, _) =>
            (accountId, 1)
        }
        .updateStateByKey[Int](updateFunction)

    // ============================================================
    // 9. Display accumulated state
    // ============================================================

    runningCounts.foreachRDD { rdd =>

      println("\n========== RUNNING ACCOUNT COUNTS ==========")

      if (rdd.isEmpty()) {

        println("No accumulated state available.")

      } else {

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (accountId, count) =>
              println(
                s"$accountId -> $count total transaction(s)"
              )
          }
      }
    }

    // ============================================================
    // 10. Running transaction count by type
    // ============================================================

    val runningTypeCounts: DStream[(String, Int)] =
      transactions
        .map {
          case (_, transactionType, _) =>
            (transactionType, 1)
        }
        .updateStateByKey[Int](updateFunction)

    runningTypeCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== RUNNING TRANSACTION TYPE COUNTS ==========")

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (transactionType, count) =>
              println(
                s"$transactionType -> $count total transaction(s)"
              )
          }
      }
    }

    // ============================================================
    // 11. Start streaming
    // ============================================================

    streamingContext.start()

    println("\nStreaming application started.")
    println("Waiting for transactions...")
    println("Press Ctrl+C to stop.")

    // ============================================================
    // 12. Wait for termination
    // ============================================================

    streamingContext.awaitTermination()
  }
}
