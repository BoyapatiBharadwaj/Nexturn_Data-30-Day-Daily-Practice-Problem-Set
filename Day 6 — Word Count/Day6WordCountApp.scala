import org.apache.spark.{SparkConf, SparkContext}

object Day6WordCountApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 6 - WORD COUNT
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
        .setAppName("Day6WordCountApp")
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
    println("DAY 6 - WORD COUNT")
    println("============================================================")
    println(s"Spark Master        : $master")
    println(s"Default Parallelism : ${sc.defaultParallelism}")
    println(s"Serializer          : ${conf.get("spark.serializer")}")
    println()

    // ============================================================
    // 1. CREATE SAMPLE TEXT RDD
    // ============================================================

    println("------------------------------------------------------------")
    println("1. CREATE SAMPLE TEXT RDD")
    println("------------------------------------------------------------")

    val sentences =
      List(
        "Apache Spark is powerful",
        "Spark is fast",
        "Scala works with Spark",
        "Spark makes data processing easier"
      )

    val textRDD =
      sc.parallelize(sentences)

    println("Input sentences:")

    textRDD.collect().foreach(
      println
    )

    println()

    // ============================================================
    // 2. FLATMAP - SPLIT TEXT INTO WORDS
    // ============================================================

    println("------------------------------------------------------------")
    println("2. FLATMAP - SPLIT TEXT INTO WORDS")
    println("------------------------------------------------------------")

    val wordsRDD =
      textRDD.flatMap { line =>
        line.split("\\s+")
      }

    println(
      s"Words: ${wordsRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 3. MAP - CREATE (WORD, 1)
    // ============================================================

    println("------------------------------------------------------------")
    println("3. MAP - CREATE (WORD, 1)")
    println("------------------------------------------------------------")

    val wordPairsRDD =
      wordsRDD.map { word =>
        (word, 1)
      }

    println(
      s"Word pairs: ${wordPairsRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 4. REDUCEBYKEY - COUNT WORDS
    // ============================================================

    println("------------------------------------------------------------")
    println("4. REDUCEBYKEY - COUNT WORDS")
    println("------------------------------------------------------------")

    val wordCountsRDD =
      wordPairsRDD.reduceByKey(
        (count1, count2) => count1 + count2
      )

    println("Word counts:")

    wordCountsRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(
            f"$word%-15s -> $count"
          )
      }

    println()

    // ============================================================
    // 5. CASE-INSENSITIVE WORD COUNT
    // ============================================================

    println("------------------------------------------------------------")
    println("5. CASE-INSENSITIVE WORD COUNT")
    println("------------------------------------------------------------")

    val caseInsensitiveWordsRDD =
      textRDD.flatMap { line =>

        line
          .toLowerCase
          .split("\\s+")
      }

    val caseInsensitivePairsRDD =
      caseInsensitiveWordsRDD.map { word =>
        (word, 1)
      }

    val caseInsensitiveCountsRDD =
      caseInsensitivePairsRDD.reduceByKey(
        _ + _
      )

    println("Case-insensitive word counts:")

    caseInsensitiveCountsRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(
            f"$word%-15s -> $count"
          )
      }

    println()

    // ============================================================
    // 6. REMOVE PUNCTUATION
    // ============================================================

    println("------------------------------------------------------------")
    println("6. REMOVE PUNCTUATION")
    println("------------------------------------------------------------")

    val punctuationTextRDD =
      sc.parallelize(
        List(
          "Spark is powerful!",
          "Spark is fast.",
          "Scala, Spark and Hadoop!",
          "Data processing is important."
        )
      )

    val cleanedWordsRDD =
      punctuationTextRDD
        .flatMap { line =>

          line
            .toLowerCase
            .replaceAll("[^a-z0-9\\s]", "")
            .split("\\s+")
        }
        .filter(
          word => word.nonEmpty
        )

    println(
      s"Cleaned words: ${cleanedWordsRDD.collect().mkString(", ")}"
    )

    println()

    // ============================================================
    // 7. COMPLETE CLEAN WORD COUNT
    // ============================================================

    println("------------------------------------------------------------")
    println("7. COMPLETE CLEAN WORD COUNT")
    println("------------------------------------------------------------")

    val cleanedWordCountsRDD =
      cleanedWordsRDD
        .map(
          word => (word, 1)
        )
        .reduceByKey(
          _ + _
        )

    println("Clean word counts:")

    cleanedWordCountsRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(
            f"$word%-15s -> $count"
          )
      }

    println()

    // ============================================================
    // 8. LOAD APPLICATION LOG
    // ============================================================

    println("------------------------------------------------------------")
    println("8. LOAD APPLICATION LOG")
    println("------------------------------------------------------------")

    val rawLogRDD =
      sc.textFile(
        "data/application.log"
      )

    val logRDD =
      rawLogRDD.filter(
        line => line.trim.nonEmpty
      )

    println(
      s"Log lines: ${logRDD.count()}"
    )

    println()

    // ============================================================
    // 9. CLEAN APPLICATION LOG WORDS
    // ============================================================

    println("------------------------------------------------------------")
    println("9. CLEAN APPLICATION LOG WORDS")
    println("------------------------------------------------------------")

    val logWordsRDD =
      logRDD.flatMap { line =>

        line
          .toLowerCase
          .replaceAll("[^a-z0-9\\s]", "")
          .split("\\s+")
      }
      .filter(
        word => word.nonEmpty
      )

    println(
      s"Number of words: ${logWordsRDD.count()}"
    )

    println(
      s"First words: ${logWordsRDD.take(20).mkString(", ")}"
    )

    println()

    // ============================================================
    // 10. LOG WORD COUNT
    // ============================================================

    println("------------------------------------------------------------")
    println("10. LOG WORD COUNT")
    println("------------------------------------------------------------")

    val logWordCountsRDD =
      logWordsRDD
        .map(
          word => (word, 1)
        )
        .reduceByKey(
          _ + _
        )

    println("Log word counts:")

    logWordCountsRDD
      .collect()
      .sortBy {
        case (word, count) =>
          word
      }
      .foreach {
        case (word, count) =>
          println(
            f"$word%-20s -> $count"
          )
      }

    println()

    // ============================================================
    // 11. TOP 10 MOST FREQUENT WORDS
    // ============================================================

    println("------------------------------------------------------------")
    println("11. TOP 10 MOST FREQUENT WORDS")
    println("------------------------------------------------------------")

    val top10Words =
      logWordCountsRDD
        .map {
          case (word, count) =>
            (count, word)
        }
        .sortByKey(
          ascending = false
        )
        .take(10)

    println(
      "Top 10 most frequent words:"
    )

    top10Words.zipWithIndex.foreach {
      case ((count, word), index) =>

        println(
          f"${index + 1}%2d. $word%-20s -> $count"
        )
    }

    println()

    // ============================================================
    // 12. EXPLAIN WORD COUNT PIPELINE
    // ============================================================

    println("------------------------------------------------------------")
    println("12. WORD COUNT PIPELINE")
    println("------------------------------------------------------------")

    println(
      "STEP 1: flatMap"
    )

    println(
      "Convert each line into individual words."
    )

    println()

    println(
      "STEP 2: map"
    )

    println(
      "Convert each word into (word, 1)."
    )

    println()

    println(
      "STEP 3: reduceByKey"
    )

    println(
      "Combine values belonging to the same word."
    )

    println()

    println(
      "Pipeline:"
    )

    println(
      "Text -> flatMap -> words -> map -> (word, 1)"
    )

    println(
      "     -> reduceByKey -> (word, count)"
    )

    println()

    // ============================================================
    // 13. TRANSFORMATION AND ACTION SUMMARY
    // ============================================================

    println("------------------------------------------------------------")
    println("13. TRANSFORMATIONS AND ACTIONS")
    println("------------------------------------------------------------")

    println("TRANSFORMATIONS:")
    println("  flatMap")
    println("  map")
    println("  filter")
    println("  reduceByKey")
    println("  sortByKey")

    println()

    println("ACTIONS:")
    println("  count")
    println("  collect")
    println("  first")
    println("  take")

    println()

    println(
      "Transformations are lazy."
    )

    println(
      "Actions trigger Spark execution."
    )

    println()

    // ============================================================
    // FINAL SUMMARY
    // ============================================================

    println("============================================================")
    println("DAY 6 SUMMARY")
    println("============================================================")

    println(
      s"Log lines             : ${logRDD.count()}"
    )

    println(
      s"Log words             : ${logWordsRDD.count()}"
    )

    println(
      s"Unique words          : ${logWordCountsRDD.count()}"
    )

    println(
      "Word Count Pipeline   : flatMap -> map -> reduceByKey"
    )

    println(
      "Case insensitive      : YES"
    )

    println(
      "Punctuation removed   : YES"
    )

    println(
      "Empty words removed   : YES"
    )

    println(
      "Top 10 words          : Generated"
    )

    println()

    println("============================================================")
    println("DAY 6 COMPLETE")
    println("============================================================")

    sc.stop()
  }
}
