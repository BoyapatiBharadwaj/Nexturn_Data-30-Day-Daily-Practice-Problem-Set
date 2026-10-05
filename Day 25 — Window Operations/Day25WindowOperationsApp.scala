import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day25WindowOperationsApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. Spark configuration
    // ============================================================

    val conf = new SparkConf()
      .setAppName("Day25 Window Operations")
      .setMaster("local[2]")

    // ============================================================
    // 2. StreamingContext
    //
    // Batch interval = 5 seconds
    // ============================================================

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("WARN")

    streamingContext.checkpoint("checkpoint")

    println("\n==============================================")
    println("DAY 25 - WINDOW OPERATIONS")
    println("==============================================")

    println("Batch interval  : 5 seconds")
    println("Window size     : 10 minutes")
    println("Slide interval  : 5 seconds")
    println("Socket          : localhost:9999")

    // ============================================================
    // 3. Create socket stream
    // ============================================================

    val rawStream =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )

    // ============================================================
    // 4. Parse transactions
    //
    // Input:
    // account_id,transaction_type,amount
    //
    // Example:
    // A1001,DEPOSIT,5000
    // ============================================================

    val transactions =
      rawStream.flatMap { line =>

        val fields =
          line.trim.split(",")

        if (fields.length == 3) {

          val accountId =
            fields(0).trim

          val transactionType =
            fields(1).trim.toUpperCase

          try {

            val amount =
              fields(2).trim.toDouble

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
    // 5. CURRENT BATCH COUNT
    //
    // Stateless result.
    // Counts transactions only in current 5-second batch.
    // ============================================================

    val currentBatchCount =
      transactions
        .map(_ => 1)
        .reduce(_ + _)

    currentBatchCount.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== CURRENT BATCH COUNT ==========")

        println(
          s"Transactions in current 5-second batch = ${rdd.first()}"
        )
      }
    }

    // ============================================================
    // 6. countByWindow
    //
    // Window = 10 minutes
    // Slide  = 5 seconds
    //
    // 600 seconds / 5 seconds = 120 batches
    // ============================================================

    val transactionsPer10Minutes =
      transactions
        .map(_ => 1)
        .countByWindow(
          Seconds(600),
          Seconds(5)
        )

    transactionsPer10Minutes.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val count =
          rdd.first()

        println(
          s"10-minute rolling transaction count = $count"
        )
      }
    }

    // ============================================================
    // 7. REDUCE BY KEY AND WINDOW
    //
    // Key = account_id
    // Value = transaction amount
    //
    // Calculates rolling transaction amount per account.
    // ============================================================

    val accountAmounts =
      transactions.map {
        case (accountId, _, amount) =>
          (accountId, amount)
      }

    val rollingAccountSales =
      accountAmounts.reduceByKeyAndWindow(
        (amount1: Double, amount2: Double) =>
          amount1 + amount2,
        Seconds(600),
        Seconds(5)
      )

    // ============================================================
    // 8. Display rolling sales per account
    // ============================================================

    rollingAccountSales.foreachRDD { rdd =>

      println("\n========== 10-MINUTE ROLLING SALES ==========")

      if (rdd.isEmpty()) {

        println("No transactions in current window.")

      } else {

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (accountId, amount) =>
              println(
                f"$accountId -> ₹$amount%.2f"
              )
          }
      }
    }

    // ============================================================
    // 9. Calculate rolling sales across ALL accounts
    // ============================================================

    val rollingTotalSales =
      transactions
        .map {
          case (_, _, amount) =>
            amount
        }
        .reduceByWindow(
          (amount1: Double, amount2: Double) =>
            amount1 + amount2,
          Seconds(600),
          Seconds(5)
        )

    // ============================================================
    // 10. Sudden transaction increase detection
    //
    // Example threshold:
    // 10-minute transaction count >= 10
    // ============================================================

    transactionsPer10Minutes.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val count =
          rdd.first()

        println("\n========== TRANSACTION ACTIVITY MONITOR ==========")

        if (count >= 10) {

          println(
            s"ALERT: Sudden transaction activity detected! " +
              s"$count transactions in the last 10 minutes."
          )

        } else {

          println(
            s"Normal activity: $count transactions " +
              s"in the last 10 minutes."
          )
        }
      }
    }

    // ============================================================
    // 11. Display total rolling sales
    // ============================================================

    rollingTotalSales.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== TOTAL ROLLING SALES ==========")

        println(
          f"Last 10 minutes sales = ₹${rdd.first()}%.2f"
        )
      }
    }

    // ============================================================
    // 12. Start streaming
    // ============================================================

    streamingContext.start()

    println("\nStreaming application started.")
    println("Waiting for transactions...")
    println("Press Ctrl+C to stop.")

    // ============================================================
    // 13. Wait
    // ============================================================

    streamingContext.awaitTermination()
  }
}
