import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Day10PartitioningApp {

  def printPartitions[T](name: String, rdd: org.apache.spark.rdd.RDD[T]): Unit = {
    println(s"$name -> ${rdd.getNumPartitions} partitions")
  }

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 10 - PARTITIONING
    // ============================================================

    val conf = new SparkConf()
      .setAppName("Day10 Partitioning")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println()
    println("============================================================")
    println("              DAY 10 - PARTITIONING")
    println("============================================================")


    // ============================================================
    // 1. CREATE RDD WITH 2 PARTITIONS
    // ============================================================

    println()
    println("1. CREATE RDD WITH 2 PARTITIONS")
    println("------------------------------------------------------------")

    val rdd2 = sc.parallelize(1 to 20, 2)

    printPartitions("rdd2", rdd2)

    println("Data:")
    rdd2.collect().foreach(println)


    // ============================================================
    // 2. INSPECT PARTITION CONTENT
    // ============================================================

    println()
    println("2. INSPECT PARTITION CONTENT")
    println("------------------------------------------------------------")

    rdd2.mapPartitionsWithIndex {
      case (partitionId, iterator) =>
        Iterator(
          s"Partition $partitionId -> ${iterator.mkString("[", ", ", "]")}"
        )
    }.collect().foreach(println)


    // ============================================================
    // 3. REPARTITION - INCREASE
    // ============================================================

    println()
    println("3. REPARTITION - INCREASE 2 -> 4")
    println("------------------------------------------------------------")

    val rdd4 = rdd2.repartition(4)

    printPartitions("rdd4", rdd4)

    rdd4.mapPartitionsWithIndex {
      case (partitionId, iterator) =>
        Iterator(
          s"Partition $partitionId -> ${iterator.mkString("[", ", ", "]")}"
        )
    }.collect().foreach(println)


    // ============================================================
    // 4. REPARTITION - INCREASE 4 -> 8
    // ============================================================

    println()
    println("4. REPARTITION - INCREASE 4 -> 8")
    println("------------------------------------------------------------")

    val rdd8 = rdd4.repartition(8)

    printPartitions("rdd8", rdd8)


    // ============================================================
    // 5. REPARTITION - DECREASE
    // ============================================================

    println()
    println("5. REPARTITION - DECREASE 8 -> 4")
    println("------------------------------------------------------------")

    val repartitionedDown = rdd8.repartition(4)

    printPartitions(
      "repartitionedDown",
      repartitionedDown
    )


    // ============================================================
    // 6. COALESCE - DECREASE
    // ============================================================

    println()
    println("6. COALESCE - DECREASE 8 -> 4")
    println("------------------------------------------------------------")

    val coalesced = rdd8.coalesce(4)

    printPartitions(
      "coalesced",
      coalesced
    )


    // ============================================================
    // 7. COMPARE REPARTITION AND COALESCE
    // ============================================================

    println()
    println("7. REPARTITION vs COALESCE")
    println("------------------------------------------------------------")

    println(
      """
      repartition(4):
        - Can increase or decrease partitions
        - Generally performs a shuffle
        - Redistributes data across partitions

      coalesce(4):
        - Mainly used to decrease partitions
        - Usually avoids a full shuffle
        - Combines existing partitions
      """
    )


    // ============================================================
    // 8. DEFAULT PARALLELISM
    // ============================================================

    println()
    println("8. DEFAULT PARALLELISM")
    println("------------------------------------------------------------")

    println(
      s"Spark default parallelism: ${sc.defaultParallelism}"
    )


    // ============================================================
    // 9. TEXT FILE PARTITIONS
    // ============================================================

    println()
    println("9. TEXT FILE PARTITIONS")
    println("------------------------------------------------------------")

    val customerRDD =
      sc.textFile("data/customers.txt")

    printPartitions(
      "customerRDD",
      customerRDD
    )

    println(
      s"Customer records: ${customerRDD.count()}"
    )


    // ============================================================
    // 10. SCENARIO - TOO FEW PARTITIONS
    // ============================================================

    println()
    println("10. SCENARIO - DATASET WITH TOO FEW PARTITIONS")
    println("------------------------------------------------------------")

    val largeDataset =
      sc.parallelize(1 to 100000, 2)

    printPartitions(
      "largeDataset before optimization",
      largeDataset
    )

    println(
      "Problem: large dataset is using only 2 partitions."
    )

    println(
      "With only 2 partitions, only 2 tasks can process the stage concurrently."
    )


    // ============================================================
    // 11. OPTIMIZE TOO FEW PARTITIONS
    // ============================================================

    println()
    println("11. INCREASE PARTITIONS")
    println("------------------------------------------------------------")

    val optimizedDataset =
      largeDataset.repartition(8)

    printPartitions(
      "optimizedDataset",
      optimizedDataset
    )

    println(
      "The dataset now has more partitions and can expose more parallelism."
    )


    // ============================================================
    // 12. PROCESS OPTIMIZED DATA
    // ============================================================

    println()
    println("12. PROCESS OPTIMIZED DATA")
    println("------------------------------------------------------------")

    val processed =
      optimizedDataset
        .filter(_ % 2 == 0)
        .map(_ * 10)

    println(
      s"Processed partitions: ${processed.getNumPartitions}"
    )

    println(
      s"Processed count: ${processed.count()}"
    )


    // ============================================================
    // 13. PAIR RDD
    // ============================================================

    println()
    println("13. CREATE PAIR RDD")
    println("------------------------------------------------------------")

    val salesRDD =
      sc.parallelize(
        Seq(
          ("Laptop", 50000),
          ("Mouse", 500),
          ("Laptop", 50000),
          ("Keyboard", 1500),
          ("Mouse", 500),
          ("Monitor", 12000),
          ("Laptop", 50000),
          ("Keyboard", 1500),
          ("Mouse", 500)
        ),
        2
      )

    printPartitions(
      "salesRDD",
      salesRDD
    )

    salesRDD.collect().foreach(println)


    // ============================================================
    // 14. partitionBy
    // ============================================================

    println()
    println("14. partitionBy - HASH PARTITIONING")
    println("------------------------------------------------------------")

    val partitionedSalesRDD =
      salesRDD.partitionBy(
        new HashPartitioner(4)
      )

    printPartitions(
      "partitionedSalesRDD",
      partitionedSalesRDD
    )

    println(
      s"Partitioner: ${partitionedSalesRDD.partitioner}"
    )


    // ============================================================
    // 15. INSPECT PAIR RDD PARTITIONS
    // ============================================================

    println()
    println("15. INSPECT PARTITIONED PAIR RDD")
    println("------------------------------------------------------------")

    partitionedSalesRDD
      .mapPartitionsWithIndex {
        case (partitionId, iterator) =>
          Iterator(
            s"Partition $partitionId -> ${iterator.mkString("[", ", ", "]")}"
          )
      }
      .collect()
      .foreach(println)


    // ============================================================
    // 16. REDUCE BY KEY AFTER PARTITIONING
    // ============================================================

    println()
    println("16. REDUCE BY KEY")
    println("------------------------------------------------------------")

    val totalSales =
      partitionedSalesRDD
        .reduceByKey(_ + _)

    totalSales.collect()
      .sortBy(_._1)
      .foreach(println)


    // ============================================================
    // 17. CUSTOMER PAIR RDD
    // ============================================================

    println()
    println("17. CUSTOMER PAIR RDD")
    println("------------------------------------------------------------")

    val customerPairs =
      customerRDD.map { line =>

        val fields = line.split(",")

        val customerId = fields(0)
        val city = fields(2)
        val amount = fields(3).toDouble

        (customerId, (city, amount))
      }

    printPartitions(
      "customerPairs",
      customerPairs
    )


    // ============================================================
    // 18. PARTITION CUSTOMERS BY KEY
    // ============================================================

    println()
    println("18. PARTITION CUSTOMERS BY CUSTOMER ID")
    println("------------------------------------------------------------")

    val partitionedCustomers =
      customerPairs.partitionBy(
        new HashPartitioner(4)
      )

    printPartitions(
      "partitionedCustomers",
      partitionedCustomers
    )

    println(
      s"Customer partitioner: ${partitionedCustomers.partitioner}"
    )


    // ============================================================
    // 19. SIMULATE TOO MANY PARTITIONS
    // ============================================================

    println()
    println("19. TOO MANY PARTITIONS")
    println("------------------------------------------------------------")

    val manyPartitions =
      sc.parallelize(1 to 100, 20)

    printPartitions(
      "manyPartitions",
      manyPartitions
    )

    val fewerPartitions =
      manyPartitions.coalesce(5)

    printPartitions(
      "fewerPartitions",
      fewerPartitions
    )


    // ============================================================
    // 20. PARTITION OPTIMIZATION SUMMARY
    // ============================================================

    println()
    println("20. PARTITION OPTIMIZATION SUMMARY")
    println("------------------------------------------------------------")

    println(
      """
      Situation 1:
        Large dataset + too few partitions
        -> Consider repartition()
        -> Increase parallelism

      Situation 2:
        Small dataset + too many partitions
        -> Consider coalesce()
        -> Reduce task overhead

      Situation 3:
        Repeated Pair RDD operations using the same key
        -> Consider partitionBy()

      Important:
        More partitions are NOT always better.
        Too many partitions can create task scheduling overhead.
      """
    )


    // ============================================================
    // 21. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 10 COMPLETE")
    println("============================================================")

    println(
      """
      Topics completed:

      1. Partition count inspection
      2. Partition contents
      3. repartition
      4. coalesce
      5. Default parallelism
      6. Too few partitions
      7. Increasing parallelism
      8. Pair RDD partitioning
      9. HashPartitioner
      10. partitionBy
      11. Partition optimization
      """
    )

    sc.stop()
  }
}
