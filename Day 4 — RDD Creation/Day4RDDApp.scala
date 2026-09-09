import org.apache.spark.{SparkConf, SparkContext}

object Day4RDDApp {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // Spark Configuration
    // ------------------------------------------------------------

    val cores =
      if (args.nonEmpty) args(0)
      else "2"

    val master = s"local[$cores]"

    val conf = new SparkConf()
      .setAppName("Day4RDDApp")
      .setMaster(master)

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println()
    println("============================================================")
    println("DAY 4 - RDD CREATION")
    println("============================================================")
    println(s"Spark Master       : $master")
    println(s"Default Parallelism: ${sc.defaultParallelism}")
    println()

    // ------------------------------------------------------------
    // 1. CREATE RDD FROM SCALA COLLECTION
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("1. RDD FROM SCALA COLLECTION")
    println("------------------------------------------------------------")

    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numberRDD = sc.parallelize(numbers)

    println(s"Original numbers: ${numberRDD.collect().mkString(", ")}")
    println(s"Partitions      : ${numberRDD.getNumPartitions}")
    println()

    // ------------------------------------------------------------
    // 2. MAP
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("2. MAP")
    println("------------------------------------------------------------")

    val squaredRDD = numberRDD.map(number => number * number)

    println(
      s"Squared numbers: ${squaredRDD.collect().mkString(", ")}"
    )
    println()

    // ------------------------------------------------------------
    // 3. FILTER
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("3. FILTER")
    println("------------------------------------------------------------")

    val evenRDD = numberRDD.filter(number => number % 2 == 0)

    println(
      s"Even numbers: ${evenRDD.collect().mkString(", ")}"
    )
    println()

    // ------------------------------------------------------------
    // 4. FLATMAP
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("4. FLATMAP")
    println("------------------------------------------------------------")

    val sentences = List(
      "Apache Spark is fast",
      "RDDs are distributed",
      "Scala works with Spark"
    )

    val sentenceRDD = sc.parallelize(sentences)

    val wordsRDD = sentenceRDD.flatMap(sentence => sentence.split(" "))

    println(
      s"Words: ${wordsRDD.collect().mkString(", ")}"
    )
    println()

    // ------------------------------------------------------------
    // 5. CREATE RDD FROM TEXT FILE
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("5. RDD FROM TEXT FILE")
    println("------------------------------------------------------------")

    val customerRDD =
      sc.textFile("data/customers.txt")

    println(s"Customer records: ${customerRDD.count()}")
    println(s"Customer partitions: ${customerRDD.getNumPartitions}")

    customerRDD.take(5).foreach(println)

    println()

    // ------------------------------------------------------------
    // 6. PROCESS CUSTOMER DATA USING MAP
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("6. MAP CUSTOMER DATA")
    println("------------------------------------------------------------")

    val customerNamesRDD =
      customerRDD.map(line => {
        val fields = line.split(",")
        fields(1)
      })

    println(
      s"Customer names: ${customerNamesRDD.collect().mkString(", ")}"
    )

    println()

    // ------------------------------------------------------------
    // 7. FILTER CUSTOMER DATA
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("7. FILTER CUSTOMERS FROM HYDERABAD")
    println("------------------------------------------------------------")

    val hyderabadCustomers =
      customerRDD.filter(line => line.endsWith("Hyderabad"))

    hyderabadCustomers.collect().foreach(println)

    println()

    // ------------------------------------------------------------
    // 8. FLATMAP CUSTOMER DATA
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("8. FLATMAP CUSTOMER DATA")
    println("------------------------------------------------------------")

    val customerWords =
      customerRDD.flatMap(line => line.split(","))

    println(
      s"Customer fields: ${customerWords.take(15).mkString(", ")}"
    )

    println()

    // ------------------------------------------------------------
    // 9. SALES RDD
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("9. SALES TRANSACTIONS")
    println("------------------------------------------------------------")

    val salesRDD =
      sc.textFile("data/sales.txt")

    val salesAmounts =
      salesRDD.map(line => {
        val fields = line.split(",")
        fields(1).toDouble
      })

    println(
      s"Sales amounts: ${salesAmounts.collect().mkString(", ")}"
    )

    println()

    // ------------------------------------------------------------
    // 10. CALCULATE TOTAL SALES
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("10. TOTAL SALES")
    println("------------------------------------------------------------")

    val totalSales =
      salesAmounts.sum()

    println(f"Total Sales: $$${totalSales}%.2f")

    println()

    // ------------------------------------------------------------
    // 11. SALES USING REDUCE
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("11. TOTAL SALES USING REDUCE")
    println("------------------------------------------------------------")

    val totalSalesUsingReduce =
      salesAmounts.reduce(_ + _)

    println(
      f"Total Sales using reduce: $$${totalSalesUsingReduce}%.2f"
    )

    println()

    // ------------------------------------------------------------
    // 12. INSPECT PARTITIONS
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("12. PARTITION INSPECTION")
    println("------------------------------------------------------------")

    println(
      s"Number RDD partitions   : ${numberRDD.getNumPartitions}"
    )

    println(
      s"Customer RDD partitions : ${customerRDD.getNumPartitions}"
    )

    println(
      s"Sales RDD partitions    : ${salesRDD.getNumPartitions}"
    )

    println()

    // ------------------------------------------------------------
    // 13. SHOW DATA INSIDE EACH PARTITION
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("13. DATA INSIDE EACH CUSTOMER PARTITION")
    println("------------------------------------------------------------")

    val partitionData =
      customerRDD.mapPartitionsWithIndex {
        (partitionIndex, iterator) => {

          val records =
            iterator.toList

          Iterator(
            s"Partition $partitionIndex -> ${records.mkString(" | ")}"
          )
        }
      }

    partitionData.collect().foreach(println)

    println()

    // ------------------------------------------------------------
    // 14. EXPLAIN DEFAULT PARALLELISM
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("14. DEFAULT PARALLELISM")
    println("------------------------------------------------------------")

    println(
      s"Spark defaultParallelism = ${sc.defaultParallelism}"
    )

    println(
      "For local mode, default parallelism is generally based on the"
    )

    println(
      "number of available local execution threads."
    )

    println()

    // ------------------------------------------------------------
    // 15. LARGE CUSTOMER FILE SCENARIO
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("15. LARGE CUSTOMER FILE SCENARIO")
    println("------------------------------------------------------------")

    val largeCustomerRDD =
      sc.textFile(
        "data/customers.txt",
        minPartitions = 4
      )

    println(
      s"Requested minimum partitions: 4"
    )

    println(
      s"Actual partitions: ${largeCustomerRDD.getNumPartitions}"
    )

    val customersPerPartition =
      largeCustomerRDD.mapPartitionsWithIndex {
        (partitionIndex, iterator) => {

          val count = iterator.size

          Iterator(
            s"Partition $partitionIndex contains $count records"
          )
        }
      }

    customersPerPartition.collect().foreach(println)

    println()

    // ------------------------------------------------------------
    // 16. PROCESS LARGE CUSTOMER FILE
    // ------------------------------------------------------------

    println("------------------------------------------------------------")
    println("16. PROCESS CUSTOMERS IN PARALLEL")
    println("------------------------------------------------------------")

    val processedCustomers =
      largeCustomerRDD
        .filter(line => line.nonEmpty)
        .map(line => {
          val fields = line.split(",")

          val id = fields(0)
          val name = fields(1)
          val city = fields(2)

          s"Customer $id -> $name -> $city"
        })

    processedCustomers.take(10).foreach(println)

    println()

    // ------------------------------------------------------------
    // FINISH
    // ------------------------------------------------------------

    println("============================================================")
    println("DAY 4 COMPLETE")
    println("============================================================")

    sc.stop()
  }
}
