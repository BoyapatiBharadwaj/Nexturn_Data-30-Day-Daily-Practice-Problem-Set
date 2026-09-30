import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day20FileFormatsApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Day20 - File Formats and Output")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("=" * 90)
    println("DAY 20 - FILE FORMATS AND OUTPUT")
    println("=" * 90)
    println()


    // ============================================================
    // 2. CREATE OUTPUT DIRECTORIES
    // ============================================================

    val csvOutput = "output/csv_sales"
    val jsonOutput = "output/json_sales"
    val parquetOutput = "output/parquet_sales"
    val partitionedOutput = "output/daily_sales"
    val repartitionedOutput = "output/repartitioned_sales"
    val coalescedOutput = "output/coalesced_sales"


    // ============================================================
    // 3. READ CSV
    // ============================================================

    println("=" * 90)
    println("1. READ CSV")
    println("=" * 90)

    val salesCSV =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/daily_sales.csv")

    salesCSV.show(10, false)

    println("CSV Schema:")
    salesCSV.printSchema()

    println(
      s"CSV record count = ${salesCSV.count()}"
    )


    // ============================================================
    // 4. PREPARE SALES DATA
    // ============================================================

    println()
    println("=" * 90)
    println("2. PREPARE SALES DATA")
    println("=" * 90)

    val sales =
      salesCSV
        .withColumn(
          "sale_date",
          to_date(col("sale_date"))
        )
        .withColumn(
          "total_amount",
          col("quantity") * col("unit_price")
        )
        .withColumn(
          "year",
          year(col("sale_date"))
        )
        .withColumn(
          "month",
          month(col("sale_date"))
        )
        .withColumn(
          "day",
          dayofmonth(col("sale_date"))
        )

    sales.show(10, false)

    sales.printSchema()


    // ============================================================
    // 5. WRITE CSV
    // ============================================================

    println()
    println("=" * 90)
    println("3. WRITE CSV")
    println("=" * 90)

    sales
      .drop("year", "month", "day")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(csvOutput)

    println(
      s"CSV written to: $csvOutput"
    )


    // ============================================================
    // 6. READ WRITTEN CSV
    // ============================================================

    println()
    println("=" * 90)
    println("4. READ WRITTEN CSV")
    println("=" * 90)

    val writtenCSV =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvOutput)

    writtenCSV.show(10, false)

    println(
      s"Records read from written CSV = ${writtenCSV.count()}"
    )


    // ============================================================
    // 7. WRITE JSON
    // ============================================================

    println()
    println("=" * 90)
    println("5. WRITE JSON")
    println("=" * 90)

    sales
      .drop("year", "month", "day")
      .write
      .mode("overwrite")
      .json(jsonOutput)

    println(
      s"JSON written to: $jsonOutput"
    )


    // ============================================================
    // 8. READ JSON
    // ============================================================

    println()
    println("=" * 90)
    println("6. READ JSON")
    println("=" * 90)

    val writtenJSON =
      spark.read
        .json(jsonOutput)

    writtenJSON.show(10, false)

    println("JSON Schema:")
    writtenJSON.printSchema()

    println(
      s"Records read from JSON = ${writtenJSON.count()}"
    )


    // ============================================================
    // 9. WRITE PARQUET
    // ============================================================

    println()
    println("=" * 90)
    println("7. WRITE PARQUET")
    println("=" * 90)

    sales
      .write
      .mode("overwrite")
      .parquet(parquetOutput)

    println(
      s"Parquet written to: $parquetOutput"
    )


    // ============================================================
    // 10. READ PARQUET
    // ============================================================

    println()
    println("=" * 90)
    println("8. READ PARQUET")
    println("=" * 90)

    val writtenParquet =
      spark.read
        .parquet(parquetOutput)

    writtenParquet.show(10, false)

    println("Parquet Schema:")
    writtenParquet.printSchema()

    println(
      s"Records read from Parquet = ${writtenParquet.count()}"
    )


    // ============================================================
    // 11. COMPARE FILE FORMATS
    // ============================================================

    println()
    println("=" * 90)
    println("9. FILE FORMAT COMPARISON")
    println("=" * 90)

    println(
      """
        |CSV
        |---
        |Text-based.
        |Human-readable.
        |Simple and widely supported.
        |Does not store Spark schema in the same way as Parquet.
        |
        |
        |JSON
        |----
        |Semi-structured.
        |Useful for nested and API-style data.
        |Usually larger than columnar formats for analytics.
        |
        |
        |PARQUET
        |-------
        |Columnar storage format.
        |Stores schema information.
        |Efficient for analytical queries.
        |Supports column pruning and predicate pushdown.
        |Commonly used in data lakes and lakehouses.
        |"""
        .stripMargin
    )


    // ============================================================
    // 12. PARTITIONED OUTPUT
    // ============================================================

    println()
    println("=" * 90)
    println("10. PARTITIONED OUTPUT")
    println("=" * 90)

    sales
      .write
      .mode("overwrite")
      .partitionBy(
        "year",
        "month",
        "day"
      )
      .parquet(partitionedOutput)

    println(
      s"Partitioned Parquet written to: $partitionedOutput"
    )


    // ============================================================
    // 13. READ PARTITIONED DATA
    // ============================================================

    println()
    println("=" * 90)
    println("11. READ PARTITIONED DATA")
    println("=" * 90)

    val partitionedSales =
      spark.read
        .parquet(partitionedOutput)

    partitionedSales
      .orderBy(
        col("sale_date"),
        col("sale_id")
      )
      .show(20, false)


    // ============================================================
    // 14. PARTITIONED DIRECTORY STRUCTURE
    // ============================================================

    println()
    println("=" * 90)
    println("12. EXPECTED PARTITION DIRECTORY STRUCTURE")
    println("=" * 90)

    println(
      """
        |output/daily_sales/
        |
        |  year=2026/
        |    |
        |    +-- month=9/
        |         |
        |         +-- day=1/
        |         |     +-- part-xxxxx.parquet
        |         |
        |         +-- day=2/
        |         |     +-- part-xxxxx.parquet
        |         |
        |         +-- day=3/
        |         |     +-- part-xxxxx.parquet
        |         |
        |         +-- ...
        |
        |
        |The exact file names are generated by Spark.
        |"""
        .stripMargin
    )


    // ============================================================
    // 15. PARTITION PRUNING
    // ============================================================

    println()
    println("=" * 90)
    println("13. PARTITION PRUNING")
    println("=" * 90)

    println(
      """
        |If data is partitioned by:
        |
        |year
        |month
        |day
        |
        |and we query:
        |
        |year = 2026
        |AND month = 9
        |AND day = 5
        |
        |Spark can avoid scanning unrelated partition directories.
        |
        |This is called partition pruning.
        |"""
        .stripMargin
    )

    val oneDaySales =
      partitionedSales
        .filter(
          col("year") === 2026 &&
          col("month") === 9 &&
          col("day") === 5
        )

    oneDaySales.show(100, false)


    // ============================================================
    // 16. REPARTITION BEFORE WRITING
    // ============================================================

    println()
    println("=" * 90)
    println("14. REPARTITION BEFORE WRITING")
    println("=" * 90)

    println(
      s"Original partitions = ${sales.rdd.getNumPartitions}"
    )

    val repartitionedSales =
      sales.repartition(4)

    println(
      s"After repartition(4) = ${repartitionedSales.rdd.getNumPartitions}"
    )

    repartitionedSales
      .drop("year", "month", "day")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(repartitionedOutput)

    println(
      s"Repartitioned data written to: $repartitionedOutput"
    )


    // ============================================================
    // 17. COALESCE BEFORE WRITING
    // ============================================================

    println()
    println("=" * 90)
    println("15. COALESCE BEFORE WRITING")
    println("=" * 90)

    val coalescedSales =
      sales.coalesce(2)

    println(
      s"After coalesce(2) = ${coalescedSales.rdd.getNumPartitions}"
    )

    coalescedSales
      .drop("year", "month", "day")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(coalescedOutput)

    println(
      s"Coalesced data written to: $coalescedOutput"
    )


    // ============================================================
    // 18. REPARTITION VS COALESCE
    // ============================================================

    println()
    println("=" * 90)
    println("16. REPARTITION VS COALESCE")
    println("=" * 90)

    println(
      """
        |repartition(N)
        |---------------
        |
        |Changes the number of partitions.
        |
        |Can increase or decrease partitions.
        |
        |Generally causes a shuffle.
        |
        |
        |coalesce(N)
        |-----------
        |
        |Primarily used to reduce partitions.
        |
        |Usually avoids a full shuffle.
        |
        |
        |Example:
        |
        |8 partitions
        |    |
        |    +-- repartition(4)
        |    |       |
        |    |       +--> shuffle
        |    |
        |    +-- coalesce(4)
        |            |
        |            +--> generally avoids full shuffle
        |"""
        .stripMargin
    )


    // ============================================================
    // 19. NUMBER OF OUTPUT FILES
    // ============================================================

    println()
    println("=" * 90)
    println("17. NUMBER OF OUTPUT FILES")
    println("=" * 90)

    println(
      """
        |Spark generally creates output files based on
        |the number of partitions being written.
        |
        |For example:
        |
        |4 partitions
        |     |
        |     v
        |Approximately 4 part files
        |
        |
        |Example:
        |
        |part-00000-xxxxx.parquet
        |part-00001-xxxxx.parquet
        |part-00002-xxxxx.parquet
        |part-00003-xxxxx.parquet
        |
        |
        |Spark also creates metadata files such as:
        |
        |_SUCCESS
        |
        |The exact number of files can depend on the
        |partitioning and write operation.
        |"""
        .stripMargin
    )


    // ============================================================
    // 20. PARTITION COUNT
    // ============================================================

    println()
    println("=" * 90)
    println("18. PARTITION COUNT ANALYSIS")
    println("=" * 90)

    println(
      s"Sales partitions = ${sales.rdd.getNumPartitions}"
    )

    println(
      s"Repartitioned sales partitions = ${
        repartitionedSales.rdd.getNumPartitions
      }"
    )

    println(
      s"Coalesced sales partitions = ${
        coalescedSales.rdd.getNumPartitions
      }"
    )


    // ============================================================
    // 21. DAILY SALES ANALYSIS
    // ============================================================

    println()
    println("=" * 90)
    println("19. DAILY SALES ANALYSIS")
    println("=" * 90)

    sales
      .groupBy(
        "year",
        "month",
        "day"
      )
      .agg(
        count("*").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        sum("total_amount").alias("total_sales"),
        avg("total_amount").alias("average_sale")
      )
      .orderBy(
        col("year"),
        col("month"),
        col("day")
      )
      .show(100, false)


    // ============================================================
    // 22. MONTHLY SALES
    // ============================================================

    println()
    println("=" * 90)
    println("20. MONTHLY SALES")
    println("=" * 90)

    sales
      .groupBy(
        "year",
        "month"
      )
      .agg(
        count("*").alias("transaction_count"),
        sum("total_amount").alias("total_sales"),
        avg("total_amount").alias("average_sale")
      )
      .orderBy(
        col("year"),
        col("month")
      )
      .show(100, false)


    // ============================================================
    // 23. PRODUCT SALES
    // ============================================================

    println()
    println("=" * 90)
    println("21. PRODUCT SALES")
    println("=" * 90)

    sales
      .groupBy(
        "product",
        "category"
      )
      .agg(
        sum("quantity").alias("total_quantity"),
        sum("total_amount").alias("total_sales")
      )
      .orderBy(
        col("total_sales").desc
      )
      .show(100, false)


    // ============================================================
    // 24. EXPLAIN PARTITIONED WRITE
    // ============================================================

    println()
    println("=" * 90)
    println("22. HOW PARTITIONED OUTPUT WORKS")
    println("=" * 90)

    println(
      """
        |Input DataFrame
        |       |
        |       v
        |partitionBy(year, month, day)
        |       |
        |       v
        |Spark groups rows by partition columns
        |       |
        |       v
        |Directory structure
        |
        |year=2026/
        |    month=9/
        |        day=1/
        |        day=2/
        |        day=3/
        |
        |
        |Partition columns are stored in the directory path.
        |
        |The Parquet files inside each directory contain
        |the actual records.
        |"""
        .stripMargin
    )


    // ============================================================
    // 25. FILE FORMAT RECOMMENDATION FOR ANALYTICS
    // ============================================================

    println()
    println("=" * 90)
    println("23. FILE FORMAT FOR ANALYTICS")
    println("=" * 90)

    println(
      """
        |For analytical workloads, Parquet is often preferred
        |over CSV/JSON because it is a columnar format and
        |works efficiently with Spark SQL.
        |
        |
        |Typical data lake pattern:
        |
        |Raw ingestion
        |     |
        |     v
        |CSV / JSON
        |     |
        |     v
        |Transformation
        |     |
        |     v
        |Parquet
        |     |
        |     v
        |Analytics
        |"""
        .stripMargin
    )


    // ============================================================
    // 26. VIVA SUMMARY
    // ============================================================

    println()
    println("=" * 90)
    println("24. DAY 20 VIVA SUMMARY")
    println("=" * 90)

    println(
      """
        |Q1. What is CSV?
        |
        |A:
        |CSV is a text-based tabular file format where values
        |are separated by delimiters such as commas.
        |
        |
        |Q2. What is JSON?
        |
        |A:
        |JSON is a semi-structured text format commonly used
        |for APIs and nested data.
        |
        |
        |Q3. What is Parquet?
        |
        |A:
        |Parquet is a columnar storage format designed for
        |efficient analytical workloads.
        |
        |
        |Q4. Why is Parquet useful in Spark?
        |
        |A:
        |It supports columnar storage, schema information,
        |compression and efficient analytical reads.
        |
        |
        |Q5. What does partitionBy do during writing?
        |
        |A:
        |It organizes output into directory partitions based
        |on the specified column values.
        |
        |
        |Q6. What does this produce?
        |
        |partitionBy("year", "month", "day")
        |
        |A:
        |
        |year=2026/
        |  month=9/
        |    day=1/
        |    day=2/
        |    day=3/
        |
        |
        |Q7. What determines the number of output files?
        |
        |A:
        |The number of partitions being written is a major
        |factor in the number of part files.
        |
        |
        |Q8. What does repartition() do?
        |
        |A:
        |It changes the number of partitions and generally
        |causes a shuffle.
        |
        |
        |Q9. What does coalesce() do?
        |
        |A:
        |It is mainly used to reduce the number of partitions
        |and generally avoids a full shuffle.
        |
        |
        |Q10. Why shouldn't we create too many output files?
        |
        |A:
        |Too many small files create metadata and filesystem
        |overhead and can reduce query performance.
        |
        |
        |Q11. What is partition pruning?
        |
        |A:
        |Partition pruning allows Spark to skip partition
        |directories that cannot contain rows matching the filter.
        |
        |
        |Q12. Why partition daily sales by year/month/day?
        |
        |A:
        |It organizes data by time and allows queries for specific
        |dates or periods to avoid scanning unrelated partitions.
        |"""
        .stripMargin
    )


    // ============================================================
    // 27. FINAL SUMMARY
    // ============================================================

    println()
    println("=" * 90)
    println("DAY 20 COMPLETED")
    println("=" * 90)

    println(
      """
        |Topics completed:
        |
        |[✓] Read CSV
        |[✓] Write CSV
        |[✓] Read JSON
        |[✓] Write JSON
        |[✓] Read Parquet
        |[✓] Write Parquet
        |[✓] Partitioned output
        |[✓] year/month/day partitioning
        |[✓] Repartition
        |[✓] Coalesce
        |[✓] Output file counts
        |[✓] part-* files
        |[✓] _SUCCESS
        |[✓] Partition pruning
        |[✓] Daily sales analytics
        |[✓] File format comparison
        |"""
        .stripMargin
    )

    println()

    spark.stop()
  }
}
