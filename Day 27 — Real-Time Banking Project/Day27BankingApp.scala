import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.HashPartitioner

case class Transaction(
  transactionId: String,
  accountId: String,
  branchId: String,
  transactionType: String,
  amount: Double,
  timestamp: String
)

case class BranchRisk(
  branchId: String,
  location: String,
  riskLevel: String
)

object Day27BankingApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day27-RealTimeBanking")
      .setMaster("local[4]")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.checkpoint("checkpoint")

    // --------------------------------------------------
    // APPLICATION HEADER
    // --------------------------------------------------

    println()
    println("================================================")
    println("        DAY 27 - REAL-TIME BANKING")
    println("================================================")
    println()
    println("Batch interval : 5 seconds")
    println("Window size    : 30 seconds")
    println("Slide interval : 5 seconds")
    println("Socket         : localhost:9999")
    println()

    // --------------------------------------------------
    // SMALL REFERENCE DATA
    // --------------------------------------------------

    val branchRiskData = Map(
      "B001" -> BranchRisk("B001", "HYDERABAD", "LOW"),
      "B002" -> BranchRisk("B002", "DELHI", "MEDIUM"),
      "B003" -> BranchRisk("B003", "MUMBAI", "HIGH"),
      "B004" -> BranchRisk("B004", "BANGALORE", "LOW"),
      "B005" -> BranchRisk("B005", "CHENNAI", "HIGH")
    )

    val broadcastBranchRisk:
      Broadcast[Map[String, BranchRisk]] =
      streamingContext.sparkContext.broadcast(branchRiskData)

    // --------------------------------------------------
    // ACCUMULATOR
    // --------------------------------------------------

    val suspiciousTransactionCounter:
      LongAccumulator =
      streamingContext.sparkContext.longAccumulator(
        "SuspiciousTransactions"
      )

    // --------------------------------------------------
    // SOCKET STREAM
    // --------------------------------------------------

    val inputStream =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )

    // --------------------------------------------------
    // PARSE TRANSACTIONS
    // --------------------------------------------------

    val transactions = inputStream.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 6) {

        try {

          Some(
            Transaction(
              parts(0).trim,
              parts(1).trim,
              parts(2).trim,
              parts(3).trim,
              parts(4).trim.toDouble,
              parts(5).trim
            )
          )

        } catch {
          case _: Exception => None
        }

      } else {
        None
      }
    }

    // --------------------------------------------------
    // CACHE / PERSIST
    // --------------------------------------------------

    val cachedTransactions =
      transactions.persist(StorageLevel.MEMORY_ONLY)

    // --------------------------------------------------
    // 1. AGGREGATE TRANSACTIONS BY ACCOUNT
    // --------------------------------------------------

    val accountTotals =
      cachedTransactions
        .map(t => (t.accountId, t.amount))
        .reduceByKey(_ + _)

    accountTotals.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("       ACCOUNT TRANSACTION TOTALS")
        println("----------------------------------------------")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, total) =>
              println(
                f"Account=$account%-8s | Total Amount=₹$total%.2f"
              )
          }
      }
    }

    // --------------------------------------------------
    // 2. DETECT SUSPICIOUS TRANSACTION BURSTS
    // --------------------------------------------------

    val accountTransactionCounts =
      cachedTransactions
        .map(t => (t.accountId, 1))

    val burstWindow =
      accountTransactionCounts
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          Seconds(30),
          Seconds(5)
        )

    val suspiciousAccounts =
      burstWindow.filter {
        case (_, count) =>
          count >= 3
      }

    suspiciousAccounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==============================================")
        println("        SUSPICIOUS TRANSACTION BURST")
        println("==============================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, count) =>

              suspiciousTransactionCounter.add(count)

              println(
                s"ALERT | Account=$account | $count transactions in last 30 seconds"
              )
          }
      }
    }

    // --------------------------------------------------
    // 3. JOIN WITH BRANCH/RISK DATA
    // --------------------------------------------------

    val enrichedTransactions =
      cachedTransactions.map { transaction =>

        val branch =
          broadcastBranchRisk.value.get(
            transaction.branchId
          )

        val location =
          branch.map(_.location).getOrElse("UNKNOWN")

        val risk =
          branch.map(_.riskLevel).getOrElse("UNKNOWN")

        (
          transaction.accountId,
          transaction.amount,
          transaction.transactionType,
          transaction.branchId,
          location,
          risk
        )
      }

    enrichedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("       ENRICHED BANK TRANSACTIONS")
        println("----------------------------------------------")

        rdd.collect().foreach {

          case (
                account,
                amount,
                transactionType,
                branchId,
                location,
                risk
              ) =>

            println(
              f"Account=$account%-8s | " +
              f"Amount=₹$amount%.2f | " +
              f"Type=$transactionType%-10s | " +
              f"Branch=$branchId | " +
              f"Location=$location%-10s | " +
              f"Risk=$risk"
            )
        }
      }
    }

    // --------------------------------------------------
    // 4. HIGH-RISK TRANSACTION ALERT
    // --------------------------------------------------

    val highRiskTransactions =
      cachedTransactions.filter { transaction =>

        broadcastBranchRisk.value
          .get(transaction.branchId)
          .exists(_.riskLevel == "HIGH")
      }

    highRiskTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("          HIGH-RISK TRANSACTIONS")
        println("----------------------------------------------")

        rdd.collect().foreach { transaction =>

          suspiciousTransactionCounter.add(1)

          println(
            f"RISK ALERT | Account=${transaction.accountId}%-8s | " +
            f"Branch=${transaction.branchId} | " +
            f"Amount=₹${transaction.amount}%.2f | " +
            f"Type=${transaction.transactionType}"
          )
        }
      }
    }

    // --------------------------------------------------
    // 5. PARTITIONING
    // --------------------------------------------------

    val partitionedTransactions =
      cachedTransactions.map(t => (t.accountId, t.amount))

    val repartitionedTransactions =
      partitionedTransactions.transform { rdd =>

        rdd.partitionBy(
          new HashPartitioner(4)
        )
      }

    repartitionedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("           PARTITION INFORMATION")
        println("----------------------------------------------")

        println(
          s"Number of partitions = ${rdd.getNumPartitions}"
        )
      }
    }

    // --------------------------------------------------
    // 6. ACCUMULATOR SUMMARY
    // --------------------------------------------------

    val summaryStream =
      cachedTransactions.map(_ => 1)

    summaryStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("----------------------------------------------")
        println("          BANKING STREAM SUMMARY")
        println("----------------------------------------------")

        println(
          s"Transactions in current batch = ${rdd.count()}"
        )

        println(
          s"Suspicious transaction counter = " +
          suspiciousTransactionCounter.value
        )
      }
    }

    // --------------------------------------------------
    // START STREAMING
    // --------------------------------------------------

    println("----------------------------------------------")
    println("Streaming application started.")
    println("Waiting for banking transactions...")
    println("----------------------------------------------")
    println()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
