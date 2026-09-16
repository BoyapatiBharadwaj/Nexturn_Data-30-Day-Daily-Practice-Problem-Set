import org.apache.spark.{SparkConf, SparkContext}

object Day7LineageFaultToleranceApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 7 - IMMUTABILITY, LINEAGE AND FAULT TOLERANCE
    // ============================================================

    val cores =
      if (args.nonEmpty) args(0)
      else "2"

    val master =
      s"local[$cores]"

    // ============================================================
    // SPARK CONFIGURATION
    // ============================================================

    val conf =
      new SparkConf()
        .setAppName("Day7LineageFaultToleranceApp")
        .setMaster(master)
        .set(
          "spark.serializer",
          "org.apache.spark.serializer.JavaSerializer"
        )

    val sc =
      new SparkContext(conf)

    sc.setLogLevel("WARN")

    println()
    println("============================================================")
    println("DAY 7 - IMMUTABILITY, LINEAGE AND FAULT TOLERANCE")
    println("============================================================")

    println(
      s"Spark Master        : $master"
    )

    println(
      s"Default Parallelism : ${sc.defaultParallelism}"
    )

    println(
      s"Serializer          : ${conf.get("spark.serializer")}"
    )

    println()

    // ============================================================
    // 1. CREATE BASE RDD
    // ============================================================

    println("------------------------------------------------------------")
    println("1. CREATE BASE RDD")
    println("------------------------------------------------------------")

    val numbers =
      List(
        1, 2, 3, 4, 5,
        6, 7, 8, 9, 10
      )

    val numberRDD =
      sc.parallelize(
        numbers,
        2
      )

    println(
      s"Original data: ${numberRDD.collect().mkString(", ")}"
    )

    println(
      s"Partitions: ${numberRDD.getNumPartitions}"
    )

    println()

    // ============================================================
    // 2. MULTI-STEP TRANSFORMATION CHAIN
    // ============================================================

    println("------------------------------------------------------------")
    println("2. MULTI-STEP TRANSFORMATION CHAIN")
    println("------------------------------------------------------------")

    val filteredRDD =
      numberRDD.filter(
        number => number % 2 == 0
      )

    val mappedRDD =
      filteredRDD.map(
        number => number * 10
      )

    val finalRDD =
      mappedRDD.filter(
        number => number > 20
      )

    println(
      "Transformation chain:"
    )

    println(
      "numberRDD"
    )

    println(
      "    |"
    )

    println(
      "    +-- filter(even numbers)"
    )

    println(
      "    |"
    )

    println(
      "    +-- map(x * 10)"
    )

    println(
      "    |"
    )

    println(
      "    +-- filter(x > 20)"
    )

    println(
      "    |"
    )

    println(
      "    v"
    )

    println(
      "finalRDD"
    )

    println()

    // ============================================================
    // 3. EXECUTE THE CHAIN
    // ============================================================

    println("------------------------------------------------------------")
    println("3. EXECUTE TRANSFORMATION CHAIN")
    println("------------------------------------------------------------")

    val finalResult =
      finalRDD.collect()

    println(
      s"Final result: ${finalResult.mkString(", ")}"
    )

    println()

    // ============================================================
    // 4. EXPLAIN IMMUTABILITY
    // ============================================================

    println("------------------------------------------------------------")
    println("4. RDD IMMUTABILITY")
    println("------------------------------------------------------------")

    println(
      "Original RDD:"
    )

    println(
      numberRDD.collect().mkString(", ")
    )

    println()

    println(
      "After filter:"
    )

    println(
      filteredRDD.collect().mkString(", ")
    )

    println()

    println(
      "After map:"
    )

    println(
      mappedRDD.collect().mkString(", ")
    )

    println()

    println(
      "After second filter:"
    )

    println(
      finalRDD.collect().mkString(", ")
    )

    println()

    println(
      "The original RDD is unchanged."
    )

    println(
      "Every transformation creates a new RDD."
    )

    println()

    // ============================================================
    // 5. DEMONSTRATE IMMUTABILITY WITH MULTIPLE REFERENCES
    // ============================================================

    println("------------------------------------------------------------")
    println("5. IMMUTABILITY DEMONSTRATION")
    println("------------------------------------------------------------")

    val originalRDD =
      sc.parallelize(
        List(10, 20, 30, 40, 50)
      )

    val greaterThan20RDD =
      originalRDD.filter(
        number => number > 20
      )

    val doubledRDD =
      originalRDD.map(
        number => number * 2
      )

    println(
      s"Original RDD : ${originalRDD.collect().mkString(", ")}"
    )

    println(
      s"Filtered RDD : ${greaterThan20RDD.collect().mkString(", ")}"
    )

    println(
      s"Doubled RDD  : ${doubledRDD.collect().mkString(", ")}"
    )

    println()

    println(
      "Both transformations started from the same original RDD."
    )

    println(
      "The original RDD was not modified."
    )

    println()

    // ============================================================
    // 6. PRINT RDD LINEAGE
    // ============================================================

    println("------------------------------------------------------------")
    println("6. RDD LINEAGE")
    println("------------------------------------------------------------")

    println(
      "Lineage of finalRDD:"
    )

    println()

    println(
      finalRDD.toDebugString
    )

    println()

    // ============================================================
    // 7. SIMPLE LINEAGE DIAGRAM
    // ============================================================

    println("------------------------------------------------------------")
    println("7. LINEAGE DIAGRAM")
    println("------------------------------------------------------------")

    println(
      """
numberRDD
    |
    | filter(x => x % 2 == 0)
    v
filteredRDD
    |
    | map(x => x * 10)
    v
mappedRDD
    |
    | filter(x => x > 20)
    v
finalRDD
    |
    | collect()
    v
RESULT
""".stripMargin
    )

    // ============================================================
    // 8. EXPLAIN LINEAGE
    // ============================================================

    println("------------------------------------------------------------")
    println("8. WHAT IS LINEAGE?")
    println("------------------------------------------------------------")

    println(
      "Lineage is the record of transformations used to create an RDD."
    )

    println(
      "Spark uses lineage to recompute lost partitions."
    )

    println(
      "Lineage allows Spark to recover data without requiring"
    )

    println(
      "every intermediate RDD to be permanently stored."
    )

    println()

    // ============================================================
    // 9. CUSTOMER DATASET
    // ============================================================

    println("------------------------------------------------------------")
    println("9. CUSTOMER DATASET")
    println("------------------------------------------------------------")

    val customerRDD =
      sc.textFile(
        "data/customers.txt"
      )

    println(
      "Customer records:"
    )

    customerRDD.collect().foreach(
      println
    )

    println()

    // ============================================================
    // 10. CUSTOMER TRANSFORMATION CHAIN
    // ============================================================

    println("------------------------------------------------------------")
    println("10. CUSTOMER TRANSFORMATION CHAIN")
    println("------------------------------------------------------------")

    val parsedCustomers =
      customerRDD
        .filter(
          line => line.trim.nonEmpty
        )
        .map { line =>

          val fields =
            line.split(",")

          (
            fields(0).toInt,
            fields(1),
            fields(2).toInt
          )
        }

    val passingCustomers =
      parsedCustomers.filter {
        case (_, _, mark) =>
          mark >= 50
      }

    val customerNames =
      passingCustomers.map {
        case (_, name, _) =>
          name
      }

    println(
      "Passing customer/student names:"
    )

    customerNames
      .collect()
      .sorted
      .foreach(
        println
      )

    println()

    // ============================================================
    // 11. CUSTOMER LINEAGE
    // ============================================================

    println("------------------------------------------------------------")
    println("11. CUSTOMER LINEAGE")
    println("------------------------------------------------------------")

    println(
      "Customer transformation lineage:"
    )

    println()

    println(
      """
customerRDD
    |
    | filter(non-empty)
    v
clean customer records
    |
    | map(parse CSV)
    v
parsedCustomers
    |
    | filter(mark >= 50)
    v
passingCustomers
    |
    | map(extract name)
    v
customerNames
""".stripMargin
    )

    println(
      "Spark stores this transformation relationship as lineage."
    )

    println()

    // ============================================================
    // 12. PARTITIONS
    // ============================================================

    println("------------------------------------------------------------")
    println("12. PARTITION INFORMATION")
    println("------------------------------------------------------------")

    println(
      s"Number RDD partitions     : ${numberRDD.getNumPartitions}"
    )

    println(
      s"Customer RDD partitions  : ${customerRDD.getNumPartitions}"
    )

    println(
      s"Final RDD partitions     : ${finalRDD.getNumPartitions}"
    )

    println()

    // ============================================================
    // 13. CONCEPTUAL EXECUTOR LOSS
    // ============================================================

    println("------------------------------------------------------------")
    println("13. CONCEPTUAL EXECUTOR LOSS")
    println("------------------------------------------------------------")

    println(
      "Imagine the following situation:"
    )

    println()

    println(
      "Executor 1 contains Partition 0."
    )

    println(
      "Executor 2 contains Partition 1."
    )

    println()

    println(
      "Suppose Executor 1 fails."
    )

    println()

    println(
      "Partition 0 is lost from Executor 1."
    )

    println()

    println(
      "Spark does NOT need to recompute the entire RDD."
    )

    println(
      "Spark uses lineage to recompute the lost Partition 0."
    )

    println()

    println(
      "Conceptually:"
    )

    println(
      """
Original RDD
   |
   +---- Partition 0 ---- Executor 1  X LOST
   |
   +---- Partition 1 ---- Executor 2  OK

                 |
                 v

Spark checks lineage

                 |
                 v

Recompute Partition 0

                 |
                 v

New executor processes Partition 0
""".stripMargin
    )

    // ============================================================
    // 14. WHAT DOES SPARK RECOMPUTE?
    // ============================================================

    println("------------------------------------------------------------")
    println("14. WHAT DOES SPARK RECOMPUTE?")
    println("------------------------------------------------------------")

    println(
      "Spark recomputes the lost partition."
    )

    println(
      "It starts from the required parent partition."
    )

    println(
      "It re-executes the transformations needed to recreate it."
    )

    println()

    println(
      "Example lineage:"
    )

    println(
      "numberRDD"
    )

    println(
      "    -> filter"
    )

    println(
      "    -> map"
    )

    println(
      "    -> filter"
    )

    println(
      "    -> finalRDD"
    )

    println()

    println(
      "If a finalRDD partition is lost,"
    )

    println(
      "Spark can recompute the necessary parent partition"
    )

    println(
      "through the transformation lineage."
    )

    println()

    // ============================================================
    // 15. FAULT TOLERANCE
    // ============================================================

    println("------------------------------------------------------------")
    println("15. FAULT TOLERANCE")
    println("------------------------------------------------------------")

    println(
      "RDD fault tolerance is based on lineage."
    )

    println()

    println(
      "If a partition is lost:"
    )

    println(
      "1. Spark detects the missing partition."
    )

    println(
      "2. Spark examines the RDD lineage."
    )

    println(
      "3. Spark identifies the required parent partition."
    )

    println(
      "4. Spark re-executes the necessary transformations."
    )

    println(
      "5. The lost partition is recreated."
    )

    println()

    // ============================================================
    // 16. IMMUTABILITY + LINEAGE + FAULT TOLERANCE
    // ============================================================

    println("------------------------------------------------------------")
    println("16. CORE CONCEPT CONNECTION")
    println("------------------------------------------------------------")

    println(
      "RDD IMMUTABILITY:"
    )

    println(
      "Transformations never modify an existing RDD."
    )

    println()

    println(
      "LINEAGE:"
    )

    println(
      "Spark remembers how each RDD was derived."
    )

    println()

    println(
      "FAULT TOLERANCE:"
    )

    println(
      "Spark uses lineage to recompute lost partitions."
    )

    println()

    println(
      "Therefore:"
    )

    println(
      "IMMUTABILITY -> LINEAGE -> RECOVERY"
    )

    println()

    // ============================================================
    // 17. FINAL SUMMARY
    // ============================================================

    println("============================================================")
    println("DAY 7 SUMMARY")
    println("============================================================")

    println(
      "RDDs are immutable."
    )

    println(
      "Transformations create new RDDs."
    )

    println(
      "Lineage records the transformation chain."
    )

    println(
      "Spark uses lineage for fault recovery."
    )

    println(
      "Only required lost partitions need to be recomputed."
    )

    println()

    println(
      "Main lineage:"
    )

    println(
      "numberRDD"
    )

    println(
      "   -> filter"
    )

    println(
      "   -> map"
    )

    println(
      "   -> filter"
    )

    println(
      "   -> finalRDD"
    )

    println()

    println(
      "============================================================"
    )

    println(
      "DAY 7 COMPLETE"
    )

    println(
      "============================================================"
    )

    sc.stop()
  }
}
