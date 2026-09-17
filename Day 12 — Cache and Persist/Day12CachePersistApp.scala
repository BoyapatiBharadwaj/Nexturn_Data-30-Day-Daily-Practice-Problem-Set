import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object Day12CachePersistApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 12 - CACHE AND PERSIST
    // ============================================================

    val conf = new SparkConf()
      .setAppName("Day12 Cache and Persist")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println()
    println("============================================================")
    println("             DAY 12 - CACHE AND PERSIST")
    println("============================================================")


    // ============================================================
    // 1. READ RAW TRANSACTION DATA
    // ============================================================

    println()
    println("1. READ RAW TRANSACTIONS")
    println("------------------------------------------------------------")

    val rawTransactions =
      sc.textFile("data/transactions.txt")

    println(
      s"Raw transaction count: ${rawTransactions.count()}"
    )

    println(
      s"Raw transaction partitions: ${rawTransactions.getNumPartitions}"
    )


    // ============================================================
    // 2. CLEAN TRANSACTION DATA
    // ============================================================

    println()
    println("2. CLEAN TRANSACTION DATA")
    println("------------------------------------------------------------")

    val cleanedTransactions =
      rawTransactions
        .map(_.trim)
        .filter(_.nonEmpty)
        .filter { line =>
          val fields = line.split(",")
          fields.length == 8
        }
        .map { line =>

          val fields = line.split(",")

          val transactionId = fields(0)
          val accountId = fields(1)
          val product = fields(2)
          val department = fields(3)
          val price = fields(4).toDouble
          val quantity = fields(5).toInt
          val location = fields(6)
          val status = fields(7)

          (
            transactionId,
            accountId,
            product,
            department,
            price,
            quantity,
            location,
            status
          )
        }
        .filter {
          case (_, _, _, _, price, quantity, _, _) =>
            price > 0 && quantity > 0
        }
        .filter {
          case (_, _, _, _, _, _, _, status) =>
            status == "Success"
        }

    println(
      "Cleaning transformations created."
    )

    println(
      "No action has been performed on the cleaned RDD yet."
    )


    // ============================================================
    // 3. CACHE CLEANED DATASET
    // ============================================================

    println()
    println("3. CACHE CLEANED TRANSACTION DATASET")
    println("------------------------------------------------------------")

    cleanedTransactions.cache()

    println(
      "cleanedTransactions.cache() called."
    )

    println(
      "The RDD will be cached when an action materializes it."
    )


    // ============================================================
    // 4. FIRST ACTION - COUNT
    // ============================================================

    println()
    println("4. REPORT 1 - TOTAL CLEAN TRANSACTIONS")
    println("------------------------------------------------------------")

    val cleanCount =
      cleanedTransactions.count()

    println(
      s"Clean successful transactions: $cleanCount"
    )

    println(
      "The first action computes the RDD and populates the cache."
    )


    // ============================================================
    // 5. SECOND ACTION - REVENUE BY PRODUCT
    // ============================================================

    println()
    println("5. REPORT 2 - REVENUE BY PRODUCT")
    println("------------------------------------------------------------")

    val revenueByProduct =
      cleanedTransactions
        .map {
          case (_, _, product, _, price, quantity, _, _) =>
            (product, price * quantity)
        }
        .reduceByKey(_ + _)

    revenueByProduct
      .collect()
      .sortBy(_._2)
      .reverse
      .foreach {
        case (product, revenue) =>
          println(
            f"$product%-12s -> ₹$revenue%.2f"
          )
      }


    // ============================================================
    // 6. THIRD ACTION - REVENUE BY ACCOUNT
    // ============================================================

    println()
    println("6. REPORT 3 - REVENUE BY ACCOUNT")
    println("------------------------------------------------------------")

    val revenueByAccount =
      cleanedTransactions
        .map {
          case (_, accountId, _, _, price, quantity, _, _) =>
            (accountId, price * quantity)
        }
        .reduceByKey(_ + _)

    revenueByAccount
      .collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, revenue) =>
          println(
            f"$accountId -> ₹$revenue%.2f"
          )
      }


    // ============================================================
    // 7. THIRD REPORT - TRANSACTIONS BY LOCATION
    // ============================================================

    println()
    println("7. REPORT 4 - TRANSACTIONS BY LOCATION")
    println("------------------------------------------------------------")

    val transactionsByLocation =
      cleanedTransactions
        .map {
          case (_, _, _, _, _, _, location, _) =>
            (location, 1)
        }
        .reduceByKey(_ + _)

    transactionsByLocation
      .collect()
      .sortBy(_._1)
      .foreach {
        case (location, count) =>
          println(
            s"$location -> $count transactions"
          )
      }


    // ============================================================
    // 8. SHOW CACHE STATUS
    // ============================================================

    println()
    println("8. CACHE STATUS")
    println("------------------------------------------------------------")

    println(
      s"cleanedTransactions.isPersisted = ${cleanedTransactions.isPersisted}"
    )

    println(
      s"cleanedTransactions.getStorageLevel = ${cleanedTransactions.getStorageLevel}"
    )


    // ============================================================
    // 9. EXPLICIT PERSIST - MEMORY_ONLY
    // ============================================================

    println()
    println("9. PERSIST USING MEMORY_ONLY")
    println("------------------------------------------------------------")

    val memoryOnlyRDD =
      rawTransactions
        .map(_.trim)
        .filter(_.nonEmpty)

    memoryOnlyRDD.persist(StorageLevel.MEMORY_ONLY)

    println(
      s"Storage level before action: ${memoryOnlyRDD.getStorageLevel}"
    )

    println(
      s"Count: ${memoryOnlyRDD.count()}"
    )

    println(
      s"Storage level after action: ${memoryOnlyRDD.getStorageLevel}"
    )


    // ============================================================
    // 10. PERSIST - MEMORY_AND_DISK
    // ============================================================

    println()
    println("10. PERSIST USING MEMORY_AND_DISK")
    println("------------------------------------------------------------")

    val memoryAndDiskRDD =
      rawTransactions
        .map(_.trim)
        .filter(_.nonEmpty)
        .map { line =>
          val fields = line.split(",")

          (
            fields(0),
            fields(1),
            fields(2),
            fields(3),
            fields(4).toDouble,
            fields(5).toInt,
            fields(6),
            fields(7)
          )
        }

    memoryAndDiskRDD.persist(
      StorageLevel.MEMORY_AND_DISK
    )

    println(
      s"Storage level before action: ${memoryAndDiskRDD.getStorageLevel}"
    )

    println(
      s"Count: ${memoryAndDiskRDD.count()}"
    )

    println(
      s"Storage level after action: ${memoryAndDiskRDD.getStorageLevel}"
    )


    // ============================================================
    // 11. PERSIST - DISK_ONLY
    // ============================================================

    println()
    println("11. PERSIST USING DISK_ONLY")
    println("------------------------------------------------------------")

    val diskOnlyRDD =
      rawTransactions
        .map(_.trim)
        .filter(_.nonEmpty)

    diskOnlyRDD.persist(StorageLevel.DISK_ONLY)

    println(
      s"Storage level before action: ${diskOnlyRDD.getStorageLevel}"
    )

    println(
      s"Count: ${diskOnlyRDD.count()}"
    )

    println(
      s"Storage level after action: ${diskOnlyRDD.getStorageLevel}"
    )


    // ============================================================
    // 12. CACHE vs PERSIST
    // ============================================================

    println()
    println("12. CACHE vs PERSIST")
    println("------------------------------------------------------------")

    println(
      """
      cache():

        cleanedTransactions.cache()

        Uses Spark's default RDD persistence level.

      persist():

        rdd.persist(StorageLevel.MEMORY_ONLY)

        rdd.persist(StorageLevel.MEMORY_AND_DISK)

        rdd.persist(StorageLevel.DISK_ONLY)

        Allows explicit control over storage level.
      """
    )


    // ============================================================
    // 13. DEMONSTRATE REUSE
    // ============================================================

    println()
    println("13. REUSE CACHED DATASET")
    println("------------------------------------------------------------")

    val reportA =
      cleanedTransactions
        .filter {
          case (_, _, _, department, _, _, _, _) =>
            department == "Electronics"
        }
        .count()

    println(
      s"Electronics transaction count: $reportA"
    )

    val reportB =
      cleanedTransactions
        .filter {
          case (_, _, _, department, _, _, _, _) =>
            department == "Furniture"
        }
        .count()

    println(
      s"Furniture transaction count: $reportB"
    )


    // ============================================================
    // 14. EXPLAIN CACHE REUSE
    // ============================================================

    println()
    println("14. CACHE REUSE CONCEPT")
    println("------------------------------------------------------------")

    println(
      """
      Without cache:

      Raw Data
         |
      Cleaning
         |
      Report 1
         |
      Cleaning may be recomputed

      Raw Data
         |
      Cleaning
         |
      Report 2
         |
      Cleaning may be recomputed

      Raw Data
         |
      Cleaning
         |
      Report 3


      With cache:

      Raw Data
         |
      Cleaning
         |
      CACHE
         |
      +----+----+----+
      |    |    |    |
     R1   R2   R3   R4

      The cleaned dataset can be reused.
      """
    )


    // ============================================================
    // 15. WHEN CACHE HURTS
    // ============================================================

    println()
    println("15. WHEN CACHING CAN HURT")
    println("------------------------------------------------------------")

    println(
      """
      Caching can hurt when:

      1. The RDD is used only once.
      2. The RDD is very large.
      3. Executor memory is limited.
      4. Cached data causes memory pressure.
      5. Cached partitions are repeatedly evicted.
      6. The computation being saved is very cheap.
      7. Too many RDDs are cached simultaneously.
      """
    )


    // ============================================================
    // 16. UNPERSIST
    // ============================================================

    println()
    println("16. UNPERSIST RDDs")
    println("------------------------------------------------------------")

    cleanedTransactions.unpersist()
    memoryOnlyRDD.unpersist()
    memoryAndDiskRDD.unpersist()
    diskOnlyRDD.unpersist()

    println(
      "Cached/persisted RDDs have been marked for removal from storage."
    )


    // ============================================================
    // 17. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 12 COMPLETE")
    println("============================================================")

    println(
      """
      Topics completed:

      1. RDD caching
      2. RDD persistence
      3. cache()
      4. persist()
      5. MEMORY_ONLY
      6. MEMORY_AND_DISK
      7. DISK_ONLY
      8. Reusing cleaned transaction data
      9. Multiple reports
      10. Cache status
      11. unpersist()
      12. When caching hurts performance
      """
    )

    sc.stop()
  }
}
