import org.apache.spark.{SparkConf, SparkContext}

object Day11BroadcastAccumulatorApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 11 - BROADCAST AND ACCUMULATORS
    // ============================================================

    val conf = new SparkConf()
      .setAppName("Day11 Broadcast and Accumulators")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println()
    println("============================================================")
    println("       DAY 11 - BROADCAST AND ACCUMULATORS")
    println("============================================================")


    // ============================================================
    // 1. CREATE SMALL PRODUCT MASTER TABLE
    // ============================================================

    println()
    println("1. PRODUCT MASTER TABLE")
    println("------------------------------------------------------------")

    val productMaster =
      Map(
        "P001" -> ("Laptop", "Electronics", 100000.0),
        "P002" -> ("Mouse", "Electronics", 5000.0),
        "P003" -> ("Keyboard", "Electronics", 10000.0),
        "P004" -> ("Monitor", "Electronics", 50000.0),
        "P005" -> ("Chair", "Furniture", 20000.0),
        "P006" -> ("Table", "Furniture", 50000.0),
        "P007" -> ("Printer", "Electronics", 30000.0),
        "P008" -> ("Desk", "Furniture", 40000.0)
      )

    productMaster.toSeq
      .sortBy(_._1)
      .foreach {
        case (productId, (name, department, maxAmount)) =>
          println(
            f"$productId -> $name, $department, max=₹$maxAmount%.2f"
          )
      }


    // ============================================================
    // 2. BROADCAST PRODUCT MASTER
    // ============================================================

    println()
    println("2. BROADCAST PRODUCT MASTER")
    println("------------------------------------------------------------")

    val broadcastProducts =
      sc.broadcast(productMaster)

    println(
      "Product master has been broadcast to executors."
    )

    println(
      s"Broadcast entries: ${broadcastProducts.value.size}"
    )


    // ============================================================
    // 3. READ TRANSACTIONS
    // ============================================================

    println()
    println("3. READ TRANSACTIONS")
    println("------------------------------------------------------------")

    val transactionRDD =
      sc.textFile("data/transactions.txt")

    println(
      s"Transaction records: ${transactionRDD.count()}"
    )


    // ============================================================
    // 4. CREATE ACCUMULATORS
    // ============================================================

    println()
    println("4. CREATE ACCUMULATORS")
    println("------------------------------------------------------------")

    val badRecordAccumulator =
      sc.longAccumulator("Bad Records")

    val unknownProductAccumulator =
      sc.longAccumulator("Unknown Products")

    val excessiveAmountAccumulator =
      sc.longAccumulator("Excessive Amounts")

    println("Accumulators created.")


    // ============================================================
    // 5. VALIDATE TRANSACTIONS USING BROADCAST DATA
    // ============================================================

    println()
    println("5. VALIDATE TRANSACTIONS")
    println("------------------------------------------------------------")

    val validatedTransactions =
      transactionRDD.map { line =>

        val fields = line.split(",")

        if (fields.length != 4) {

          badRecordAccumulator.add(1)

          ("INVALID", line, "Malformed record")

        } else {

          val transactionId = fields(0)
          val productId = fields(1)
          val amount = fields(2).toDouble
          val location = fields(3)

          val productInfo =
            broadcastProducts.value.get(productId)

          productInfo match {

            case None =>

              badRecordAccumulator.add(1)
              unknownProductAccumulator.add(1)

              (
                "INVALID",
                transactionId,
                s"Unknown product: $productId"
              )


            case Some((productName, department, maxAmount)) =>

              if (amount > maxAmount) {

                badRecordAccumulator.add(1)
                excessiveAmountAccumulator.add(1)

                (
                  "INVALID",
                  transactionId,
                  s"Amount ₹$amount exceeds maximum ₹$maxAmount for $productName"
                )

              } else {

                (
                  "VALID",
                  transactionId,
                  s"$productName | $department | ₹$amount | $location"
                )
              }
          }
        }
      }


    // ============================================================
    // 6. DISPLAY VALIDATION RESULTS
    // ============================================================

    println()
    println("6. VALIDATION RESULTS")
    println("------------------------------------------------------------")

    validatedTransactions
      .collect()
      .foreach {
        case (status, transactionId, message) =>
          println(
            f"$status%-8s | $transactionId%-6s | $message"
          )
      }


    // ============================================================
    // 7. FORCE AN ACTION
    // ============================================================

    println()
    println("7. COUNT VALID TRANSACTIONS")
    println("------------------------------------------------------------")

    val validCount =
      validatedTransactions
        .filter(_._1 == "VALID")
        .count()

    println(
      s"Valid transaction count: $validCount"
    )


    // ============================================================
    // 8. COUNT INVALID TRANSACTIONS
    // ============================================================

    println()
    println("8. INVALID TRANSACTIONS")
    println("------------------------------------------------------------")

    val invalidCount =
      validatedTransactions
        .filter(_._1 == "INVALID")
        .count()

    println(
      s"Invalid transaction count: $invalidCount"
    )


    // ============================================================
    // 9. ACCUMULATOR RESULTS
    // ============================================================

    println()
    println("9. ACCUMULATOR RESULTS")
    println("------------------------------------------------------------")

    println(
      s"Bad records: ${badRecordAccumulator.value}"
    )

    println(
      s"Unknown products: ${unknownProductAccumulator.value}"
    )

    println(
      s"Excessive amounts: ${excessiveAmountAccumulator.value}"
    )


    // ============================================================
    // 10. VALID TRANSACTION REVENUE BY PRODUCT
    // ============================================================

    println()
    println("10. VALID TRANSACTION AMOUNT BY PRODUCT")
    println("------------------------------------------------------------")

    val validTransactions =
      transactionRDD
        .map { line =>

          val fields = line.split(",")

          val transactionId = fields(0)
          val productId = fields(1)
          val amount = fields(2).toDouble
          val location = fields(3)

          (
            transactionId,
            productId,
            amount,
            location
          )
        }
        .filter {
          case (_, productId, amount, _) =>

            broadcastProducts.value.get(productId) match {

              case Some((_, _, maxAmount)) =>
                amount <= maxAmount

              case None =>
                false
            }
        }


    val validRevenueByProduct =
      validTransactions
        .map {
          case (_, productId, amount, _) =>
            (productId, amount)
        }
        .reduceByKey(_ + _)


    validRevenueByProduct
      .collect()
      .sortBy(_._1)
      .foreach {
        case (productId, revenue) =>
          val productName =
            broadcastProducts.value(productId)._1

          println(
            f"$productId -> $productName%-10s -> ₹$revenue%.2f"
          )
      }


    // ============================================================
    // 11. VALID TRANSACTION COUNT BY PRODUCT
    // ============================================================

    println()
    println("11. VALID TRANSACTION COUNT BY PRODUCT")
    println("------------------------------------------------------------")

    val validCountByProduct =
      validTransactions
        .map {
          case (_, productId, _, _) =>
            (productId, 1)
        }
        .reduceByKey(_ + _)

    validCountByProduct
      .collect()
      .sortBy(_._1)
      .foreach {
        case (productId, count) =>
          val productName =
            broadcastProducts.value(productId)._1

          println(
            s"$productId -> $productName -> $count transactions"
          )
      }


    // ============================================================
    // 12. BROADCAST LOOKUP DEMONSTRATION
    // ============================================================

    println()
    println("12. BROADCAST LOOKUP DEMONSTRATION")
    println("------------------------------------------------------------")

    val sampleProducts =
      sc.parallelize(
        Seq("P001", "P002", "P004", "P999"),
        2
      )

    val lookupResult =
      sampleProducts.map { productId =>

        broadcastProducts.value.get(productId) match {

          case Some((name, department, maxAmount)) =>
            (
              productId,
              name,
              department,
              maxAmount
            )

          case None =>
            (
              productId,
              "UNKNOWN",
              "UNKNOWN",
              0.0
            )
        }
      }

    lookupResult.collect().foreach(println)


    // ============================================================
    // 13. NORMAL DRIVER VARIABLE - CONCEPTUAL DEMONSTRATION
    // ============================================================

    println()
    println("13. NORMAL DRIVER VARIABLE")
    println("------------------------------------------------------------")

    var normalCounter = 0

    println(
      "normalCounter is a driver-side variable."
    )

    println(
      "It should NOT be used as a reliable distributed task counter."
    )

    println(
      "Use an accumulator when tasks need to contribute to a counter."
    )


    // ============================================================
    // 14. ACCUMULATOR USE
    // ============================================================

    println()
    println("14. ACCUMULATOR USE")
    println("------------------------------------------------------------")

    val evenNumberAccumulator =
      sc.longAccumulator("Even Numbers")

    sc.parallelize(1 to 20, 4).foreach { number =>

      if (number % 2 == 0) {
        evenNumberAccumulator.add(1)
      }
    }

    println(
      s"Even numbers counted using accumulator: ${evenNumberAccumulator.value}"
    )


    // ============================================================
    // 15. PARTITION INFORMATION
    // ============================================================

    println()
    println("15. PARTITION INFORMATION")
    println("------------------------------------------------------------")

    println(
      s"Transaction partitions: ${transactionRDD.getNumPartitions}"
    )

    println(
      s"Sample product partitions: ${sampleProducts.getNumPartitions}"
    )

    println(
      s"Default parallelism: ${sc.defaultParallelism}"
    )


    // ============================================================
    // 16. BROADCAST CLEANUP
    // ============================================================

    println()
    println("16. BROADCAST CLEANUP")
    println("------------------------------------------------------------")

    broadcastProducts.unpersist()

    println(
      "Broadcast variable unpersisted."
    )


    // ============================================================
    // 17. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 11 COMPLETE")
    println("============================================================")

    println(
      """
      Topics completed:

      1. Broadcast variables
      2. Small master/reference data
      3. Executor-side lookup
      4. Accumulators
      5. Bad-record counting
      6. Unknown-product counting
      7. Excessive-amount counting
      8. Driver variable limitation
      9. Transaction validation
      10. Broadcast + RDD processing
      """
    )

    sc.stop()
  }
}
