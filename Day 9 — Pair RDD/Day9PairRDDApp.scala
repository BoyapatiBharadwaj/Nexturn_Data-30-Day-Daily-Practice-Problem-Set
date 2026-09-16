import org.apache.spark.{SparkConf, SparkContext}

object Day9PairRDDApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 9 - PAIR RDD
    // ============================================================

    val conf = new SparkConf()
      .setAppName("Day9 Pair RDD")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println()
    println("============================================================")
    println("             DAY 9 - PAIR RDD PRACTICE")
    println("============================================================")


    // ============================================================
    // 1. CREATE A BASIC PAIR RDD
    // ============================================================

    println()
    println("1. BASIC PAIR RDD")
    println("------------------------------------------------------------")

    val basicPairRDD = sc.parallelize(
      Seq(
        ("Laptop", 50000),
        ("Mouse", 500),
        ("Laptop", 50000),
        ("Keyboard", 1500),
        ("Mouse", 500)
      )
    )

    basicPairRDD.collect().foreach(println)


    // ============================================================
    // 2. reduceByKey
    // ============================================================

    println()
    println("2. reduceByKey - TOTAL VALUE BY PRODUCT")
    println("------------------------------------------------------------")

    val reducedRDD = basicPairRDD.reduceByKey(_ + _)

    reducedRDD.collect()
      .sortBy(_._1)
      .foreach {
        case (product, total) =>
          println(s"$product -> $total")
      }


    // ============================================================
    // 3. groupByKey
    // ============================================================

    println()
    println("3. groupByKey - GROUP VALUES BY PRODUCT")
    println("------------------------------------------------------------")

    val groupedRDD = basicPairRDD.groupByKey()

    groupedRDD.collect()
      .sortBy(_._1)
      .foreach {
        case (product, values) =>
          println(s"$product -> ${values.toSeq.mkString("[", ", ", "]")}")
      }


    // ============================================================
    // 4. groupByKey + mapValues
    // ============================================================

    println()
    println("4. groupByKey + mapValues - TOTAL BY PRODUCT")
    println("------------------------------------------------------------")

    val groupedTotalsRDD =
      groupedRDD.mapValues(values => values.sum)

    groupedTotalsRDD.collect()
      .sortBy(_._1)
      .foreach {
        case (product, total) =>
          println(s"$product -> $total")
      }


    // ============================================================
    // 5. mapValues
    // ============================================================

    println()
    println("5. mapValues - DOUBLE QUANTITY")
    println("------------------------------------------------------------")

    val quantitiesRDD = sc.parallelize(
      Seq(
        ("Laptop", 2),
        ("Mouse", 10),
        ("Keyboard", 5),
        ("Monitor", 3)
      )
    )

    val doubledQuantities = quantitiesRDD.mapValues(_ * 2)

    doubledQuantities.collect()
      .sortBy(_._1)
      .foreach(println)


    // ============================================================
    // 6. READ SALES DATA
    // ============================================================

    println()
    println("6. READ SALES DATA")
    println("------------------------------------------------------------")

    val salesRDD = sc.textFile("data/sales.txt")

    println(s"Number of sales records: ${salesRDD.count()}")


    // ============================================================
    // 7. PARSE SALES DATA
    // ============================================================

    println()
    println("7. PARSED SALES DATA")
    println("------------------------------------------------------------")

    val parsedSalesRDD = salesRDD.map { line =>

      val fields = line.split(",")

      val transactionId = fields(0)
      val product = fields(1)
      val department = fields(2)
      val price = fields(3).toDouble
      val quantity = fields(4).toInt

      (transactionId, product, department, price, quantity)
    }

    parsedSalesRDD.take(5).foreach(println)


    // ============================================================
    // 8. REVENUE BY PRODUCT
    // ============================================================

    println()
    println("8. REVENUE BY PRODUCT")
    println("------------------------------------------------------------")

    val productRevenueRDD =
      parsedSalesRDD
        .map {
          case (_, product, _, price, quantity) =>
            (product, price * quantity)
        }
        .reduceByKey(_ + _)

    productRevenueRDD.collect()
      .sortBy(_._2)
      .reverse
      .foreach {
        case (product, revenue) =>
          println(f"$product%-12s -> ₹$revenue%.2f")
      }


    // ============================================================
    // 9. REVENUE BY DEPARTMENT
    // ============================================================

    println()
    println("9. REVENUE BY DEPARTMENT")
    println("------------------------------------------------------------")

    val departmentRevenueRDD =
      parsedSalesRDD
        .map {
          case (_, _, department, price, quantity) =>
            (department, price * quantity)
        }
        .reduceByKey(_ + _)

    departmentRevenueRDD.collect()
      .sortBy(_._2)
      .reverse
      .foreach {
        case (department, revenue) =>
          println(f"$department%-15s -> ₹$revenue%.2f")
      }


    // ============================================================
    // 10. QUANTITY SOLD BY PRODUCT
    // ============================================================

    println()
    println("10. TOTAL QUANTITY SOLD BY PRODUCT")
    println("------------------------------------------------------------")

    val productQuantityRDD =
      parsedSalesRDD
        .map {
          case (_, product, _, _, quantity) =>
            (product, quantity)
        }
        .reduceByKey(_ + _)

    productQuantityRDD.collect()
      .sortBy(_._1)
      .foreach {
        case (product, quantity) =>
          println(s"$product -> $quantity units")
      }


    // ============================================================
    // 11. groupByKey PERFORMANCE DEMONSTRATION
    // ============================================================

    println()
    println("11. reduceByKey vs groupByKey")
    println("------------------------------------------------------------")

    val performanceRDD =
      sc.parallelize(
        Seq(
          ("Laptop", 1000),
          ("Laptop", 2000),
          ("Laptop", 3000),
          ("Laptop", 4000),
          ("Mouse", 500),
          ("Mouse", 700),
          ("Mouse", 900),
          ("Keyboard", 1000),
          ("Keyboard", 1500)
        ),
        2
      )

    val startReduce = System.nanoTime()

    val reduceResult =
      performanceRDD
        .reduceByKey(_ + _)
        .collect()

    val reduceTime =
      (System.nanoTime() - startReduce) / 1e6

    println()
    println("reduceByKey result:")
    reduceResult
      .sortBy(_._1)
      .foreach(println)

    println(f"reduceByKey execution time: $reduceTime%.3f ms")


    val startGroup = System.nanoTime()

    val groupResult =
      performanceRDD
        .groupByKey()
        .mapValues(_.sum)
        .collect()

    val groupTime =
      (System.nanoTime() - startGroup) / 1e6

    println()
    println("groupByKey + mapValues result:")
    groupResult
      .sortBy(_._1)
      .foreach(println)

    println(f"groupByKey execution time: $groupTime%.3f ms")


    println()
    println("Performance concept:")
    println("reduceByKey can combine values locally before shuffle.")
    println("groupByKey moves/groups all values for each key.")
    println("For aggregation, reduceByKey is generally more efficient.")


    // ============================================================
    // 12. BANK TRANSACTION SCENARIO
    // ============================================================

    println()
    println("12. BANK TRANSACTIONS BY ACCOUNT ID")
    println("------------------------------------------------------------")

    val transactionRDD =
      sc.textFile("data/bank_transactions.txt")

    val parsedTransactions =
      transactionRDD.map { line =>

        val fields = line.split(",")

        val transactionId = fields(0)
        val accountId = fields(1)
        val amount = fields(2).toDouble
        val transactionType = fields(3)
        val method = fields(4)

        (transactionId, accountId, amount, transactionType, method)
      }


    // ============================================================
    // 13. TOTAL TRANSACTION AMOUNT BY ACCOUNT
    // ============================================================

    println()
    println("13. TOTAL TRANSACTION AMOUNT BY ACCOUNT")
    println("------------------------------------------------------------")

    val accountTotalsRDD =
      parsedTransactions
        .map {
          case (_, accountId, amount, _, _) =>
            (accountId, amount)
        }
        .reduceByKey(_ + _)

    accountTotalsRDD.collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, total) =>
          println(f"$accountId -> ₹$total%.2f")
      }


    // ============================================================
    // 14. CREDIT AMOUNT BY ACCOUNT
    // ============================================================

    println()
    println("14. CREDIT AMOUNT BY ACCOUNT")
    println("------------------------------------------------------------")

    val creditRDD =
      parsedTransactions
        .filter {
          case (_, _, _, transactionType, _) =>
            transactionType == "Credit"
        }
        .map {
          case (_, accountId, amount, _, _) =>
            (accountId, amount)
        }
        .reduceByKey(_ + _)

    creditRDD.collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, totalCredit) =>
          println(f"$accountId -> ₹$totalCredit%.2f")
      }


    // ============================================================
    // 15. DEBIT AMOUNT BY ACCOUNT
    // ============================================================

    println()
    println("15. DEBIT AMOUNT BY ACCOUNT")
    println("------------------------------------------------------------")

    val debitRDD =
      parsedTransactions
        .filter {
          case (_, _, _, transactionType, _) =>
            transactionType == "Debit"
        }
        .map {
          case (_, accountId, amount, _, _) =>
            (accountId, amount)
        }
        .reduceByKey(_ + _)

    debitRDD.collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, totalDebit) =>
          println(f"$accountId -> ₹$totalDebit%.2f")
      }


    // ============================================================
    // 16. BANK TRANSACTION GROUPING USING groupByKey
    // ============================================================

    println()
    println("16. GROUP TRANSACTIONS BY ACCOUNT")
    println("------------------------------------------------------------")

    val accountAmounts =
      parsedTransactions
        .map {
          case (_, accountId, amount, _, _) =>
            (accountId, amount)
        }

    val groupedAccounts =
      accountAmounts.groupByKey()

    groupedAccounts.collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, amounts) =>
          println(
            s"$accountId -> ${amounts.toSeq.mkString("[", ", ", "]")}"
          )
      }


    // ============================================================
    // 17. ACCOUNT TOTALS USING mapValues
    // ============================================================

    println()
    println("17. ACCOUNT TOTALS USING groupByKey + mapValues")
    println("------------------------------------------------------------")

    val accountTotalsUsingGroup =
      groupedAccounts
        .mapValues(values => values.sum)

    accountTotalsUsingGroup.collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, total) =>
          println(f"$accountId -> ₹$total%.2f")
      }


    // ============================================================
    // 18. PARTITION INFORMATION
    // ============================================================

    println()
    println("18. PARTITION INFORMATION")
    println("------------------------------------------------------------")

    println(
      s"Sales RDD partitions: ${salesRDD.getNumPartitions}"
    )

    println(
      s"Performance RDD partitions: ${performanceRDD.getNumPartitions}"
    )

    println(
      s"Transaction RDD partitions: ${transactionRDD.getNumPartitions}"
    )

    println(
      s"Default parallelism: ${sc.defaultParallelism}"
    )


    // ============================================================
    // 19. STAGE / SHUFFLE CONCEPT
    // ============================================================

    println()
    println("19. STAGE AND SHUFFLE CONCEPT")
    println("------------------------------------------------------------")

    println(
      """
      Pair RDD pipeline:

      Input
        |
        v
      map
        |
        v
      reduceByKey
        |
        |  SHUFFLE
        v
      reduce
        |
        v
      collect

      map -> narrow transformation
      reduceByKey -> wide transformation
      collect -> action

      The reduceByKey shuffle creates a stage boundary.
      """
    )


    // ============================================================
    // 20. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 9 COMPLETE")
    println("============================================================")

    println(
      """
      Topics completed:

      1. Pair RDD creation
      2. reduceByKey
      3. groupByKey
      4. mapValues
      5. Revenue by product
      6. Revenue by department
      7. Quantity by product
      8. reduceByKey vs groupByKey
      9. Bank transaction aggregation
      10. Credit/debit aggregation
      11. Partitions and default parallelism
      12. Shuffle and stage concepts
      """
    )

    sc.stop()
  }
}
