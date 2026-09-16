import org.apache.spark.{SparkConf, SparkContext}

object Day8DAGExecutionApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 8 - DAG AND SPARK EXECUTION
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
        .setAppName("Day8DAGExecutionApp")
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
    println("DAY 8 - DAG AND SPARK EXECUTION")
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

    val numbersRDD =
      sc.parallelize(
        List(
          1, 2, 3, 4, 5,
          6, 7, 8, 9, 10
        ),
        2
      )

    println(
      s"Data: ${numbersRDD.collect().mkString(", ")}"
    )

    println(
      s"Partitions: ${numbersRDD.getNumPartitions}"
    )

    println()

    // ============================================================
    // 2. NARROW TRANSFORMATION CHAIN
    // ============================================================

    println("------------------------------------------------------------")
    println("2. NARROW TRANSFORMATION CHAIN")
    println("------------------------------------------------------------")

    val evenRDD =
      numbersRDD.filter(
        x => x % 2 == 0
      )

    val doubledRDD =
      evenRDD.map(
        x => x * 2
      )

    val greaterRDD =
      doubledRDD.filter(
        x => x > 5
      )

    println(
      "Pipeline:"
    )

    println(
      "numbersRDD"
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
      "    -> greaterRDD"
    )

    println()

    println(
      s"Result: ${greaterRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 3. IDENTIFY NARROW TRANSFORMATIONS
    // ============================================================

    println("------------------------------------------------------------")
    println("3. NARROW TRANSFORMATIONS")
    println("------------------------------------------------------------")

    println(
      "filter is a narrow transformation."
    )

    println(
      "map is a narrow transformation."
    )

    println(
      "A child partition depends on a small number of parent"
    )

    println(
      "partitions, typically one parent partition."
    )

    println()

    println(
      "Therefore:"
    )

    println(
      "filter -> map -> filter"
    )

    println(
      "can execute within the same stage."
    )

    println()

    // ============================================================
    // 4. CREATE KEY-VALUE RDD
    // ============================================================

    println("------------------------------------------------------------")
    println("4. CREATE KEY-VALUE RDD")
    println("------------------------------------------------------------")

    val pairRDD =
      numbersRDD.map { number =>
        (
          number % 3,
          number
        )
      }

    println(
      "Key-value data:"
    )

    pairRDD
      .collect()
      .foreach {
        case (key, value) =>
          println(
            s"$key -> $value"
          )
      }

    println()

    // ============================================================
    // 5. REDUCEBYKEY - WIDE TRANSFORMATION
    // ============================================================

    println("------------------------------------------------------------")
    println("5. REDUCEBYKEY - WIDE TRANSFORMATION")
    println("------------------------------------------------------------")

    val reducedRDD =
      pairRDD.reduceByKey(
        _ + _
      )

    println(
      "reduceByKey requires values with the same key"
    )

    println(
      "to be combined."
    )

    println()

    println(
      "This can require a shuffle."
    )

    println(
      "Therefore reduceByKey is a wide transformation."
    )

    println()

    val reducedResult =
      reducedRDD.collect().sortBy(_._1)

    println(
      "Reduced result:"
    )

    reducedResult.foreach {
      case (key, value) =>
        println(
          s"$key -> $value"
        )
    }

    println()

    // ============================================================
    // 6. REDUCEBYKEY LINEAGE
    // ============================================================

    println("------------------------------------------------------------")
    println("6. REDUCEBYKEY LINEAGE")
    println("------------------------------------------------------------")

    println(
      """
numbersRDD
    |
    | map(number => (number % 3, number))
    v
pairRDD
    |
    | reduceByKey(_ + _)
    | 
    | ===== SHUFFLE =====
    |
    v
reducedRDD
    |
    | collect()
    v
RESULT
""".stripMargin
    )

    // ============================================================
    // 7. PREDICT STAGES
    // ============================================================

    println("------------------------------------------------------------")
    println("7. STAGE PREDICTION")
    println("------------------------------------------------------------")

    println(
      "Pipeline:"
    )

    println(
      "map -> reduceByKey -> collect"
    )

    println()

    println(
      "Predicted stages: 2"
    )

    println()

    println(
      "Stage 0:"
    )

    println(
      "map"
    )

    println()

    println(
      "Shuffle boundary"
    )

    println()

    println(
      "Stage 1:"
    )

    println(
      "reduceByKey"
    )

    println(
      "then collect result"
    )

    println()

    // ============================================================
    // 8. MULTIPLE TRANSFORMATIONS BEFORE SHUFFLE
    // ============================================================

    println("------------------------------------------------------------")
    println("8. MULTIPLE TRANSFORMATIONS BEFORE SHUFFLE")
    println("------------------------------------------------------------")

    val complexPairRDD =
      numbersRDD
        .filter(
          x => x > 2
        )
        .map(
          x => x * 10
        )
        .map { x =>
          (
            x % 3,
            x
          )
        }

    val complexReducedRDD =
      complexPairRDD.reduceByKey(
        _ + _
      )

    val complexResult =
      complexReducedRDD.collect().sortBy(_._1)

    println(
      "Pipeline:"
    )

    println(
      "numbersRDD"
    )

    println(
      "    -> filter"
    )

    println(
      "    -> map"
    )

    println(
      "    -> map"
    )

    println(
      "    -> reduceByKey"
    )

    println(
      "    -> collect"
    )

    println()

    println(
      "Predicted stages: 2"
    )

    println(
      "Reason: all narrow transformations before reduceByKey"
    )

    println(
      "can belong to the first stage."
    )

    println(
      "reduceByKey introduces the shuffle boundary."
    )

    println()

    println(
      "Result:"
    )

    complexResult.foreach {
      case (key, value) =>
        println(
          s"$key -> $value"
        )
    }

    println()

    // ============================================================
    // 9. SALES DATA
    // ============================================================

    println("------------------------------------------------------------")
    println("9. SALES DATA")
    println("------------------------------------------------------------")

    val salesRDD =
      sc.textFile(
        "data/sales.txt"
      )
      .filter(
        line => line.trim.nonEmpty
      )

    println(
      s"Sales records: ${salesRDD.count()}"
    )

    salesRDD
      .take(5)
      .foreach(
        println
      )

    println()

    // ============================================================
    // 10. PARSE SALES
    // ============================================================

    println("------------------------------------------------------------")
    println("10. PARSE SALES RECORDS")
    println("------------------------------------------------------------")

    val parsedSalesRDD =
      salesRDD.map { line =>

        val fields =
          line.split(",")

        (
          fields(0).toInt,
          fields(1),
          fields(2).toDouble,
          fields(3).toInt
        )
      }

    println(
      "Parsed sales:"
    )

    parsedSalesRDD
      .take(5)
      .foreach(
        println
      )

    println()

    // ============================================================
    // 11. NARROW SALES TRANSFORMATIONS
    // ============================================================

    println("------------------------------------------------------------")
    println("11. NARROW SALES TRANSFORMATIONS")
    println("------------------------------------------------------------")

    val validSalesRDD =
      parsedSalesRDD.filter {
        case (_, _, price, quantity) =>
          price > 0 && quantity > 0
      }

    val salesWithRevenueRDD =
      validSalesRDD.map {
        case (id, product, price, quantity) =>

          (
            product,
            price * quantity
          )
      }

    println(
      "Pipeline:"
    )

    println(
      "salesRDD"
    )

    println(
      "    -> map(parse)"
    )

    println(
      "    -> filter(valid records)"
    )

    println(
      "    -> map(calculate revenue)"
    )

    println()

    println(
      "All of these are narrow transformations."
    )

    println()

    // ============================================================
    // 12. SALES REDUCEBYKEY
    // ============================================================

    println("------------------------------------------------------------")
    println("12. SALES REDUCEBYKEY")
    println("------------------------------------------------------------")

    val productRevenueRDD =
      salesWithRevenueRDD.reduceByKey(
        _ + _
      )

    val productRevenue =
      productRevenueRDD
        .collect()
        .sortBy(_._1)

    println(
      "Revenue by product:"
    )

    productRevenue.foreach {
      case (product, revenue) =>
        println(
          f"$product%-12s -> ₹$revenue%.2f"
        )
    }

    println()

    // ============================================================
    // 13. SALES DAG
    // ============================================================

    println("------------------------------------------------------------")
    println("13. SALES DAG")
    println("------------------------------------------------------------")

    println(
      """
INPUT FILE
    |
    v
salesRDD
    |
    | map(parse)
    v
parsedSalesRDD
    |
    | filter(valid)
    v
validSalesRDD
    |
    | map(revenue)
    v
salesWithRevenueRDD
    |
    | reduceByKey
    |
    | ===== SHUFFLE =====
    |
    v
productRevenueRDD
    |
    | collect
    v
FINAL RESULT
""".stripMargin
    )

    println()

    // ============================================================
    // 14. SALES STAGE PREDICTION
    // ============================================================

    println("------------------------------------------------------------")
    println("14. SALES STAGE PREDICTION")
    println("------------------------------------------------------------")

    println(
      "Pipeline:"
    )

    println(
      "textFile -> map -> filter -> map -> reduceByKey -> collect"
    )

    println()

    println(
      "Expected number of stages: 2"
    )

    println()

    println(
      "Stage 0:"
    )

    println(
      "textFile"
    )

    println(
      "    -> map"
    )

    println(
      "    -> filter"
    )

    println(
      "    -> map"
    )

    println()

    println(
      "SHUFFLE"
    )

    println()

    println(
      "Stage 1:"
    )

    println(
      "reduceByKey"
    )

    println(
      "    -> collect"
    )

    println()

    // ============================================================
    // 15. MULTIPLE ACTIONS
    // ============================================================

    println("------------------------------------------------------------")
    println("15. MULTIPLE ACTIONS")
    println("------------------------------------------------------------")

    println(
      "Action 1: count()"
    )

    val recordCount =
      salesRDD.count()

    println(
      s"Records: $recordCount"
    )

    println()

    println(
      "Action 2: first()"
    )

    val firstRecord =
      salesRDD.first()

    println(
      s"First record: $firstRecord"
    )

    println()

    println(
      "Action 3: take(3)"
    )

    val firstThree =
      salesRDD.take(3)

    firstThree.foreach(
      println
    )

    println()

    println(
      "Each action can trigger its own job."
    )

    println(
      "Without caching, Spark may recompute the required"
    )

    println(
      "transformations for separate actions."
    )

    println()

    // ============================================================
    // 16. JOBS, STAGES, TASKS AND PARTITIONS
    // ============================================================

    println("------------------------------------------------------------")
    println("16. JOBS, STAGES, TASKS AND PARTITIONS")
    println("------------------------------------------------------------")

    println(
      "JOB:"
    )

    println(
      "A job is created when an action is called."
    )

    println()

    println(
      "STAGE:"
    )

    println(
      "A job is divided into stages around shuffle boundaries."
    )

    println()

    println(
      "TASK:"
    )

    println(
      "A task is the unit of work executed for a partition."
    )

    println()

    println(
      "PARTITION:"
    )

    println(
      "A partition is a chunk of an RDD."
    )

    println()

    println(
      "Relationship:"
    )

    println(
      "Action -> Job -> Stages -> Tasks -> Partitions"
    )

    println()

    // ============================================================
    // 17. NARROW VS WIDE
    // ============================================================

    println("------------------------------------------------------------")
    println("17. NARROW VS WIDE TRANSFORMATIONS")
    println("------------------------------------------------------------")

    println(
      "NARROW TRANSFORMATIONS:"
    )

    println(
      "  map"
    )

    println(
      "  filter"
    )

    println(
      "  flatMap"
    )

    println()

    println(
      "WIDE TRANSFORMATIONS:"
    )

    println(
      "  reduceByKey"
    )

    println(
      "  groupByKey"
    )

    println(
      "  distinct"
    )

    println()

    println(
      "Narrow:"
    )

    println(
      "No data redistribution is normally required."
    )

    println()

    println(
      "Wide:"
    )

    println(
      "Data may need to move between partitions."
    )

    println(
      "This is called a shuffle."
    )

    println()

    // ============================================================
    // 18. PARTITION AND TASK EXAMPLE
    // ============================================================

    println("------------------------------------------------------------")
    println("18. PARTITION AND TASK EXAMPLE")
    println("------------------------------------------------------------")

    val partitionedRDD =
      sc.parallelize(
        1 to 8,
        4
      )

    println(
      s"Number of partitions: ${partitionedRDD.getNumPartitions}"
    )

    println()

    println(
      "Conceptually:"
    )

    println(
      """
Partition 0 -> Task 0
Partition 1 -> Task 1
Partition 2 -> Task 2
Partition 3 -> Task 3
""".stripMargin
    )

    println(
      "Tasks are executed by Spark executors."
    )

    println()

    // ============================================================
    // 19. DAG VIEW
    // ============================================================

    println("------------------------------------------------------------")
    println("19. DAG VIEW")
    println("------------------------------------------------------------")

    println(
      """
                 DRIVER
                   |
                   | Action
                   v
                  JOB
                   |
          +--------+--------+
          |                 |
       STAGE 0           STAGE 1
          |                 |
    filter/map/...      reduceByKey
          |                 |
       Tasks             Tasks
          |                 |
    Partitions          Partitions
          |                 |
          +------SHUFFLE----+
""".stripMargin
    )

    println()

    // ============================================================
    // 20. FINAL CONCEPT SUMMARY
    // ============================================================

    println("------------------------------------------------------------")
    println("20. FINAL CONCEPT SUMMARY")
    println("------------------------------------------------------------")

    println(
      "DAG = Directed Acyclic Graph."
    )

    println(
      "Spark creates a DAG from transformations."
    )

    println(
      "An action triggers a job."
    )

    println(
      "The job is divided into stages."
    )

    println(
      "Shuffle boundaries separate stages."
    )

    println(
      "Each partition is processed by a task."
    )

    println(
      "Narrow transformations stay within a stage."
    )

    println(
      "Wide transformations can introduce shuffle."
    )

    println()

    // ============================================================
    // FINAL OUTPUT
    // ============================================================

    println("============================================================")
    println("DAY 8 SUMMARY")
    println("============================================================")

    println(
      "Main pipeline:"
    )

    println(
      "map -> filter -> map -> reduceByKey -> collect"
    )

    println()

    println(
      "Expected stages for this pipeline: 2"
    )

    println()

    println(
      "Narrow transformations:"
    )

    println(
      "map, filter, flatMap"
    )

    println()

    println(
      "Wide transformations:"
    )

    println(
      "reduceByKey, groupByKey, distinct"
    )

    println()

    println(
      "Shuffle:"
    )

    println(
      "Redistribution of data between partitions."
    )

    println()

    println(
      "============================================================")
    println("DAY 8 COMPLETE")
    println("============================================================")

    sc.stop()
  }
}
