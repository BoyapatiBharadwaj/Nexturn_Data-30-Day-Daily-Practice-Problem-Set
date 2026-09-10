import org.apache.spark.{SparkConf, SparkContext}

object Day5TransformationsActionsApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 5 - TRANSFORMATIONS AND ACTIONS
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
        .setAppName("Day5TransformationsActionsApp")
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
    println("DAY 5 - TRANSFORMATIONS AND ACTIONS")
    println("============================================================")
    println(s"Spark Master        : $master")
    println(s"Default Parallelism : ${sc.defaultParallelism}")
    println(s"Serializer          : ${conf.get("spark.serializer")}")
    println()

    // ============================================================
    // 1. CREATE BASE RDD
    // ============================================================

    println("------------------------------------------------------------")
    println("1. CREATE BASE RDD")
    println("------------------------------------------------------------")

    val numbers =
      List(
        1, 2, 3, 4, 5, 5,
        6, 7, 8, 8, 9, 10
      )

    val numberRDD =
      sc.parallelize(numbers)

    println(
      s"Original RDD: ${numberRDD.collect().mkString(", ")}"
    )

    println(
      s"Number of partitions: ${numberRDD.getNumPartitions}"
    )

    println()

    // ============================================================
    // 2. MAP - TRANSFORMATION
    // ============================================================

    println("------------------------------------------------------------")
    println("2. MAP - TRANSFORMATION")
    println("------------------------------------------------------------")

    val squaredRDD =
      numberRDD.map(
        number => number * number
      )

    println(
      s"Squared values: ${squaredRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 3. FILTER - TRANSFORMATION
    // ============================================================

    println("------------------------------------------------------------")
    println("3. FILTER - TRANSFORMATION")
    println("------------------------------------------------------------")

    val evenRDD =
      numberRDD.filter(
        number => number % 2 == 0
      )

    println(
      s"Even numbers: ${evenRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 4. FLATMAP - TRANSFORMATION
    // ============================================================

    println("------------------------------------------------------------")
    println("4. FLATMAP - TRANSFORMATION")
    println("------------------------------------------------------------")

    val sentences =
      List(
        "Apache Spark is powerful",
        "RDD processing is distributed",
        "Scala works with Spark"
      )

    val sentenceRDD =
      sc.parallelize(sentences)

    val wordsRDD =
      sentenceRDD.flatMap(
        sentence => sentence.split(" ")
      )

    println(
      s"Words: ${wordsRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 5. DISTINCT - TRANSFORMATION
    // ============================================================

    println("------------------------------------------------------------")
    println("5. DISTINCT - TRANSFORMATION")
    println("------------------------------------------------------------")

    val distinctRDD =
      numberRDD.distinct()

    val distinctValues =
      distinctRDD.collect().sorted

    println(
      s"Distinct values: ${distinctValues.mkString(", ")}"
    )

    println(
      s"Distinct count: ${distinctRDD.count()}"
    )

    println()

    // ============================================================
    // 6. UNION - TRANSFORMATION
    // ============================================================

    println("------------------------------------------------------------")
    println("6. UNION - TRANSFORMATION")
    println("------------------------------------------------------------")

    val firstRDD =
      sc.parallelize(
        List(1, 2, 3)
      )

    val secondRDD =
      sc.parallelize(
        List(4, 5, 6)
      )

    val unionRDD =
      firstRDD.union(secondRDD)

    println(
      s"First RDD : ${firstRDD.collect().mkString(", ")}"
    )

    println(
      s"Second RDD: ${secondRDD.collect().mkString(", ")}"
    )

    println(
      s"Union RDD : ${unionRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 7. COUNT - ACTION
    // ============================================================

    println("------------------------------------------------------------")
    println("7. COUNT - ACTION")
    println("------------------------------------------------------------")

    val elementCount =
      numberRDD.count()

    println(
      s"Number of elements: $elementCount"
    )

    println()

    // ============================================================
    // 8. COLLECT - ACTION
    // ============================================================

    println("------------------------------------------------------------")
    println("8. COLLECT - ACTION")
    println("------------------------------------------------------------")

    val collectedNumbers =
      numberRDD.collect()

    println(
      s"Collected values: ${collectedNumbers.mkString(", ")}"
    )

    println()

    // ============================================================
    // 9. FIRST - ACTION
    // ============================================================

    println("------------------------------------------------------------")
    println("9. FIRST - ACTION")
    println("------------------------------------------------------------")

    val firstNumber =
      numberRDD.first()

    println(
      s"First element: $firstNumber"
    )

    println()

    // ============================================================
    // 10. TAKE - ACTION
    // ============================================================

    println("------------------------------------------------------------")
    println("10. TAKE - ACTION")
    println("------------------------------------------------------------")

    val firstFive =
      numberRDD.take(5)

    println(
      s"First five elements: ${firstFive.mkString(", ")}"
    )

    println()

    // ============================================================
    // 11. REDUCE - ACTION
    // ============================================================

    println("------------------------------------------------------------")
    println("11. REDUCE - ACTION")
    println("------------------------------------------------------------")

    val total =
      numberRDD.reduce(_ + _)

    println(
      s"Sum using reduce: $total"
    )

    println()

    // ============================================================
    // 12. TRANSFORMATION CHAIN
    // ============================================================

    println("------------------------------------------------------------")
    println("12. TRANSFORMATION CHAIN")
    println("------------------------------------------------------------")

    val processedRDD =
      numberRDD
        .filter(
          number => number % 2 == 0
        )
        .map(
          number => number * 10
        )
        .distinct()

    println(
      "Transformation chain:"
    )

    println(
      "filter -> map -> distinct"
    )

    println(
      "The transformations are lazy."
    )

    println(
      "An action is required to execute the computation."
    )

    val processedResult =
      processedRDD.collect()

    println(
      s"Result: ${processedResult.sorted.mkString(", ")}"
    )

    println()

    // ============================================================
    // 13. LAZY EVALUATION DEMONSTRATION
    // ============================================================

    println("------------------------------------------------------------")
    println("13. LAZY EVALUATION")
    println("------------------------------------------------------------")

    val lazyRDD =
      numberRDD.filter { number =>

        println(
          s"Executing filter for number: $number"
        )

        number > 5
      }

    println()

    println(
      "Transformation has been defined."
    )

    println(
      "Spark has not executed the filter yet."
    )

    println()

    println(
      "Calling count() now triggers execution:"
    )

    val lazyCount =
      lazyRDD.count()

    println()

    println(
      s"Numbers greater than 5: $lazyCount"
    )

    println()

    // ============================================================
    // 14. LOAD APPLICATION LOG
    // ============================================================

    println("------------------------------------------------------------")
    println("14. LOAD APPLICATION LOG")
    println("------------------------------------------------------------")

    val rawLogRDD =
      sc.textFile(
        "data/application.log"
      )

    // Remove blank/empty lines so malformed empty records
    // cannot break later parsing operations.
    val logRDD =
      rawLogRDD.filter(
        line => line.trim.nonEmpty
      )

    val totalLogLines =
      logRDD.count()

    println(
      s"Total log lines: $totalLogLines"
    )

    println()

    // ============================================================
    // 15. FILTER ERROR MESSAGES
    // ============================================================

    println("------------------------------------------------------------")
    println("15. FILTER ERROR MESSAGES")
    println("------------------------------------------------------------")

    val errorLogs =
      logRDD.filter(
        line => line.contains("ERROR")
      )

    println(
      "ERROR messages:"
    )

    errorLogs.collect().foreach(
      println
    )

    println()

    // ============================================================
    // 16. COUNT ERROR MESSAGES
    // ============================================================

    println("------------------------------------------------------------")
    println("16. COUNT ERROR MESSAGES")
    println("------------------------------------------------------------")

    val errorCount =
      errorLogs.count()

    println(
      s"Total ERROR messages: $errorCount"
    )

    println()

    // ============================================================
    // 17. LOG LEVEL ANALYSIS
    // ============================================================

    println("------------------------------------------------------------")
    println("17. LOG LEVEL ANALYSIS")
    println("------------------------------------------------------------")

    /*
     * Log format:
     *
     * 2026-09-10 09:00:01 INFO Application started
     *          |         |       |
     *          |         |       +-- Message
     *          |         +---------- Log level
     *          +-------------------- Date + time
     *
     * Therefore fields(2) contains INFO, ERROR or WARN.
     *
     * split("\\s+", 4) makes parsing safer because the message
     * itself can contain spaces.
     */

    val logLevels =
      logRDD.flatMap { line =>

        val fields =
          line.trim.split("\\s+", 4)

        if (fields.length >= 3) {
          Some(fields(2))
        } else {
          None
        }
      }

    println(
      s"Log levels: ${logLevels.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 18. DISTINCT LOG LEVELS
    // ============================================================

    println("------------------------------------------------------------")
    println("18. DISTINCT LOG LEVELS")
    println("------------------------------------------------------------")

    val distinctLogLevels =
      logLevels.distinct()

    println(
      s"Distinct log levels: ${distinctLogLevels.collect().sorted.mkString(", ")}"
    )

    println()

    // ============================================================
    // 19. ERROR MESSAGE ANALYSIS
    // ============================================================

    println("------------------------------------------------------------")
    println("19. ERROR MESSAGE ANALYSIS")
    println("------------------------------------------------------------")

    val errorMessages =
      errorLogs.map { line =>

        val fields =
          line.trim.split("\\s+", 4)

        if (fields.length >= 4) {
          fields(3)
        } else {
          line
        }
      }

    println(
      "Error descriptions:"
    )

    errorMessages.collect().foreach(
      println
    )

    println()

    // ============================================================
    // 20. FIRST ERROR
    // ============================================================

    println("------------------------------------------------------------")
    println("20. FIRST ERROR")
    println("------------------------------------------------------------")

    val firstError =
      errorLogs.first()

    println(
      s"First ERROR: $firstError"
    )

    println()

    // ============================================================
    // 21. TAKE FIRST 3 ERRORS
    // ============================================================

    println("------------------------------------------------------------")
    println("21. FIRST 3 ERRORS")
    println("------------------------------------------------------------")

    val firstThreeErrors =
      errorLogs.take(3)

    firstThreeErrors.foreach(
      println
    )

    println()

    // ============================================================
    // 22. UNION MULTIPLE LOG SOURCES
    // ============================================================

    println("------------------------------------------------------------")
    println("22. UNION MULTIPLE LOG SOURCES")
    println("------------------------------------------------------------")

    val applicationLogs =
      sc.parallelize(
        List(
          "ERROR Application database failure",
          "INFO Application started"
        )
      )

    val serviceLogs =
      sc.parallelize(
        List(
          "ERROR Payment service failure",
          "WARN Payment service slow"
        )
      )

    val combinedLogs =
      applicationLogs.union(serviceLogs)

    println(
      "Combined logs:"
    )

    combinedLogs.collect().foreach(
      println
    )

    println()

    // ============================================================
    // 23. PARTITION INFORMATION
    // ============================================================

    println("------------------------------------------------------------")
    println("23. PARTITION INFORMATION")
    println("------------------------------------------------------------")

    println(
      s"Number RDD partitions: ${numberRDD.getNumPartitions}"
    )

    println(
      s"Log RDD partitions   : ${logRDD.getNumPartitions}"
    )

    println()

    // ============================================================
    // 24. TRANSFORMATIONS VS ACTIONS
    // ============================================================

    println("------------------------------------------------------------")
    println("24. TRANSFORMATIONS VS ACTIONS")
    println("------------------------------------------------------------")

    println(
      "TRANSFORMATIONS:"
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

    println(
      "  distinct"
    )

    println(
      "  union"
    )

    println()

    println(
      "ACTIONS:"
    )

    println(
      "  count"
    )

    println(
      "  collect"
    )

    println(
      "  first"
    )

    println(
      "  take"
    )

    println(
      "  reduce"
    )

    println()

    println(
      "Transformations are lazy."
    )

    println(
      "Actions trigger execution."
    )

    println()

    // ============================================================
    // FINAL SUMMARY
    // ============================================================

    println("============================================================")
    println("DAY 5 SUMMARY")
    println("============================================================")

    println(
      s"Original element count : $elementCount"
    )

    println(
      s"Distinct element count : ${distinctRDD.count()}"
    )

    println(
      s"Sum using reduce       : $total"
    )

    println(
      s"Log lines              : $totalLogLines"
    )

    println(
      s"ERROR messages         : $errorCount"
    )

    println()

    println(
      "Transformations are lazy:"
    )

    println(
      "map, filter, flatMap, distinct, union"
    )

    println()

    println(
      "Actions:"
    )

    println(
      "count, collect, first, take, reduce"
    )

    println()

    println("============================================================")
    println("DAY 5 COMPLETE")
    println("============================================================")

    // ============================================================
    // STOP SPARK
    // ============================================================

    sc.stop()
  }
}
