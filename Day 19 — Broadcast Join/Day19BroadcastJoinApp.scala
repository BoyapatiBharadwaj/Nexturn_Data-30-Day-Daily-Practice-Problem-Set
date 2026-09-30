import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Day19BroadcastJoinApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Day19 - Broadcast Join")
      .master("local[4]")
      .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("=" * 90)
    println("DAY 19 - BROADCAST JOIN")
    println("=" * 90)
    println()


    // ============================================================
    // 2. READ SMALL BRANCH MASTER
    // ============================================================

    val branches = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/branches.csv")

    println("=" * 90)
    println("1. BRANCH MASTER")
    println("=" * 90)

    branches.show(20, false)

    println("Branch master count:")
    println(branches.count())

    println()
    println("Branch master schema:")
    branches.printSchema()


    // ============================================================
    // 3. CREATE LARGE TRANSACTION FACT DATAFRAME
    // ============================================================

    println()
    println("=" * 90)
    println("2. CREATING LARGE TRANSACTION FACT DATAFRAME")
    println("=" * 90)

    val transactionCount = 1000000L

    val transactions =
      spark.range(0, transactionCount)
        .withColumn(
          "transaction_id",
          concat(
            lit("T"),
            lpad(col("id").cast("string"), 9, "0")
          )
        )
        .withColumn(
          "account_id",
          concat(
            lit("A"),
            lpad(
              (col("id") % 100000).cast("string"),
              6,
              "0"
            )
          )
        )
        .withColumn(
          "branch_id",
          concat(
            lit("BR"),
            lpad(
              ((col("id") % 10) + 1).cast("string"),
              3,
              "0"
            )
          )
        )
        .withColumn(
          "transaction_type",
          when(
            col("id") % 2 === 0,
            "DEPOSIT"
          ).otherwise("WITHDRAWAL")
        )
        .withColumn(
          "amount",
          round(
            (col("id") % 100000) + 100,
            2
          )
        )
        .withColumn(
          "transaction_date",
          date_add(
            lit("2026-09-01").cast("date"),
            (col("id") % 30).cast("int")
          )
        )
        .drop("id")


    println(
      s"Number of transactions = $transactionCount"
    )

    println()
    println("Transaction sample:")

    transactions.show(10, false)

    println()
    println("Transaction schema:")

    transactions.printSchema()

    println()
    println(
      s"Transaction partitions = ${transactions.rdd.getNumPartitions}"
    )


    // ============================================================
    // 4. BASIC TRANSACTION STATISTICS
    // ============================================================

    println()
    println("=" * 90)
    println("3. TRANSACTION STATISTICS")
    println("=" * 90)

    transactions
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        min("amount").alias("minimum_amount"),
        max("amount").alias("maximum_amount")
      )
      .show(false)


    // ============================================================
    // 5. NORMAL JOIN
    // ============================================================

    println()
    println("=" * 90)
    println("4. NORMAL JOIN")
    println("=" * 90)

    val normalJoin =
      transactions.join(
        branches,
        transactions("branch_id") === branches("branch_id"),
        "inner"
      )

    println("Normal join result:")

    normalJoin
      .select(
        transactions("transaction_id"),
        transactions("account_id"),
        transactions("branch_id"),
        transactions("transaction_type"),
        transactions("amount"),
        branches("branch_name"),
        branches("city"),
        branches("state"),
        branches("region")
      )
      .show(10, false)


    // ============================================================
    // 6. EXPLAIN NORMAL JOIN
    // ============================================================

    println()
    println("=" * 90)
    println("5. NORMAL JOIN EXECUTION PLAN")
    println("=" * 90)

    normalJoin.explain(true)


    // ============================================================
    // 7. EXPLICIT BROADCAST JOIN
    // ============================================================

    println()
    println("=" * 90)
    println("6. EXPLICIT BROADCAST JOIN")
    println("=" * 90)

    val broadcastJoin =
      transactions.join(
        broadcast(branches),
        transactions("branch_id") === branches("branch_id"),
        "inner"
      )

    val enrichedTransactions =
      broadcastJoin
        .select(
          transactions("transaction_id"),
          transactions("account_id"),
          transactions("branch_id"),
          transactions("transaction_type"),
          transactions("amount"),
          transactions("transaction_date"),
          branches("branch_name"),
          branches("city"),
          branches("state"),
          branches("region"),
          branches("manager")
        )

    println("Broadcast join result:")

    enrichedTransactions
      .show(10, false)


    // ============================================================
    // 8. EXPLAIN BROADCAST JOIN
    // ============================================================

    println()
    println("=" * 90)
    println("7. BROADCAST JOIN EXECUTION PLAN")
    println("=" * 90)

    broadcastJoin.explain(true)


    // ============================================================
    // 9. VERIFY BROADCAST JOIN
    // ============================================================

    println()
    println("=" * 90)
    println("8. VERIFY BROADCAST JOIN")
    println("=" * 90)

    println(
      """
        |Look for the following in the physical plan:
        |
        |BroadcastHashJoin
        |
        |and:
        |
        |BroadcastExchange
        |
        |This indicates that Spark is broadcasting the small
        |branch reference dataset.
        |"""
        .stripMargin
    )


    // ============================================================
    // 10. COUNT ENRICHED TRANSACTIONS
    // ============================================================

    println()
    println("=" * 90)
    println("9. ENRICHED TRANSACTION COUNT")
    println("=" * 90)

    val enrichedCount =
      enrichedTransactions.count()

    println(
      s"Enriched transaction count = $enrichedCount"
    )


    // ============================================================
    // 11. TRANSACTIONS BY REGION
    // ============================================================

    println()
    println("=" * 90)
    println("10. TRANSACTIONS BY REGION")
    println("=" * 90)

    enrichedTransactions
      .groupBy("region")
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount")
      )
      .orderBy(
        col("total_amount").desc
      )
      .show(100, false)


    // ============================================================
    // 12. TRANSACTIONS BY BRANCH
    // ============================================================

    println()
    println("=" * 90)
    println("11. TRANSACTIONS BY BRANCH")
    println("=" * 90)

    enrichedTransactions
      .groupBy(
        "branch_id",
        "branch_name",
        "city",
        "state",
        "region"
      )
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        max("amount").alias("maximum_transaction")
      )
      .orderBy(
        col("total_amount").desc
      )
      .show(100, false)


    // ============================================================
    // 13. TRANSACTIONS BY TRANSACTION TYPE
    // ============================================================

    println()
    println("=" * 90)
    println("12. TRANSACTION TYPE ANALYSIS")
    println("=" * 90)

    enrichedTransactions
      .groupBy("transaction_type")
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount")
      )
      .orderBy(
        col("total_amount").desc
      )
      .show(100, false)


    // ============================================================
    // 14. BROADCAST JOIN WITH FILTER
    // ============================================================

    println()
    println("=" * 90)
    println("13. BROADCAST JOIN + FILTER")
    println("=" * 90)

    val highValueTransactions =
      transactions
        .filter(
          col("amount") >= 50000
        )
        .join(
          broadcast(branches),
          transactions("branch_id") === branches("branch_id"),
          "inner"
        )
        .select(
          transactions("transaction_id"),
          transactions("account_id"),
          transactions("branch_id"),
          transactions("amount"),
          branches("branch_name"),
          branches("city"),
          branches("region")
        )

    highValueTransactions
      .orderBy(
        col("amount").desc
      )
      .show(20, false)


    // ============================================================
    // 15. BROADCAST JOIN WITH SQL
    // ============================================================

    println()
    println("=" * 90)
    println("14. BROADCAST JOIN USING SQL")
    println("=" * 90)

    transactions.createOrReplaceTempView("transactions")

    branches.createOrReplaceTempView("branches")

    val sqlBroadcastJoin =
      spark.sql(
        """
          |SELECT /*+ BROADCAST(b) */
          |    t.transaction_id,
          |    t.account_id,
          |    t.branch_id,
          |    t.transaction_type,
          |    t.amount,
          |    t.transaction_date,
          |    b.branch_name,
          |    b.city,
          |    b.state,
          |    b.region,
          |    b.manager
          |FROM transactions t
          |INNER JOIN branches b
          |    ON t.branch_id = b.branch_id
          |"""
          .stripMargin
      )

    sqlBroadcastJoin
      .show(10, false)

    println()
    println("SQL broadcast join physical plan:")

    sqlBroadcastJoin.explain(true)


    // ============================================================
    // 16. BROADCAST JOIN USE CASE
    // ============================================================

    println()
    println("=" * 90)
    println("15. REAL-WORLD USE CASE")
    println("=" * 90)

    println(
      """
        |Large Fact Table:
        |
        |Transactions
        |-----------------------------
        |Millions / Billions of rows
        |
        |
        |Small Reference Table:
        |
        |Branches
        |-----------------------------
        |Hundreds / Thousands of rows
        |
        |
        |Join:
        |
        |Transactions.branch_id
        |            =
        |Branches.branch_id
        |
        |
        |Result:
        |
        |Transaction + Branch Information
        |
        |
        |This is an ideal pattern for a broadcast join when
        |the branch master is sufficiently small to fit
        |comfortably in executor memory.
        |"""
        .stripMargin
    )


    // ============================================================
    // 17. BROADCAST JOIN VS SHUFFLE SORT MERGE JOIN
    // ============================================================

    println()
    println("=" * 90)
    println("16. BROADCAST JOIN VS SHUFFLE SORT MERGE JOIN")
    println("=" * 90)

    println(
      """
        |BROADCAST HASH JOIN
        |-------------------
        |
        |Small table
        |     |
        |     v
        |Broadcast
        |     |
        |     +--------+--------+
        |     |        |        |
        |     v        v        v
        | Executor  Executor  Executor
        |     |        |        |
        |     +--------+--------+
        |              |
        |              v
        |             JOIN
        |
        |
        |SHUFFLE SORT MERGE JOIN
        |----------------------
        |
        |Large table            Large table
        |     |                     |
        |     v                     v
        | Partition               Partition
        |     |                     |
        |     v                     v
        |  Shuffle               Shuffle
        |     |                     |
        |     v                     v
        |    Sort                 Sort
        |       \                 /
        |        \               /
        |         v             v
        |             MERGE
        |
        |
        |Broadcast:
        |
        |  Small dataset is copied to executors.
        |
        |
        |Shuffle Sort Merge:
        |
        |  Both datasets participate in repartitioning
        |  and sorting based on the join key.
        |"""
        .stripMargin
    )


    // ============================================================
    // 18. WHEN BROADCAST JOIN IS APPROPRIATE
    // ============================================================

    println()
    println("=" * 90)
    println("17. WHEN IS BROADCAST JOIN APPROPRIATE?")
    println("=" * 90)

    println(
      """
        |Use broadcast join when:
        |
        |1. One side of the join is small.
        |
        |2. The small dataset can comfortably fit in the
        |   memory available to executors.
        |
        |3. The large dataset should avoid a full shuffle.
        |
        |4. The small dataset is reused across many records
        |   of the large dataset.
        |
        |Examples:
        |
        |Transaction Fact + Branch Master
        |
        |Sales Fact + Product Master
        |
        |Orders + Small Customer Reference
        |
        |Events + Country/Region Lookup
        |
        |Employees + Department Master
        |"""
        .stripMargin
    )


    // ============================================================
    // 19. WHEN NOT TO USE BROADCAST JOIN
    // ============================================================

    println()
    println("=" * 90)
    println("18. WHEN SHOULD YOU AVOID BROADCAST JOIN?")
    println("=" * 90)

    println(
      """
        |Avoid broadcast when:
        |
        |1. The supposedly small table is actually very large.
        |
        |2. Broadcasting it would consume excessive executor memory.
        |
        |3. The reference table cannot comfortably fit on
        |   the executors.
        |
        |4. The dataset size changes significantly and may
        |   become too large to broadcast.
        |
        |
        |Potential problem:
        |
        |Large table
        |      +
        |Broadcast large table
        |      |
        |      v
        |Executor memory pressure
        |      |
        |      v
        |Possible performance degradation or failure
        |"""
        .stripMargin
    )


    // ============================================================
    // 20. AUTOMATIC BROADCAST
    // ============================================================

    println()
    println("=" * 90)
    println("19. AUTOMATIC BROADCAST JOIN")
    println("=" * 90)

    println(
      """
        |Spark SQL has an automatic broadcast mechanism.
        |
        |Current configuration:
        |
        |spark.sql.autoBroadcastJoinThreshold
        |
        |In this application:
        |
        |10 MB
        |
        |If a relation is sufficiently small, Spark's optimizer
        |may automatically choose a broadcast join.
        |
        |Explicit:
        |
        |broadcast(branches)
        |
        |forces the broadcast hint for this join.
        |"""
        .stripMargin
    )


    // ============================================================
    // 21. CHECK BRANCH MASTER SIZE
    // ============================================================

    println()
    println("=" * 90)
    println("20. BRANCH MASTER SIZE CHECK")
    println("=" * 90)

    println(
      s"Number of branch records = ${branches.count()}"
    )

    println(
      """
        |The number of rows alone does not determine whether
        |a dataset is safe to broadcast.
        |
        |Actual serialized size and executor memory matter.
        |"""
        .stripMargin
    )


    // ============================================================
    // 22. FINAL DATA ENGINEERING SCENARIO
    // ============================================================

    println()
    println("=" * 90)
    println("21. FINAL BANKING SCENARIO")
    println("=" * 90)

    println(
      """
        |                    BRANCH MASTER
        |                         |
        |                         |
        |                  Small Reference Data
        |                         |
        |                         v
        |                  BROADCAST JOIN
        |                         |
        |                         |
        |                         v
        |                    +---------+
        |                    |         |
        |                    | SPARK   |
        |                    |EXECUTOR |
        |                    |         |
        |                    +---------+
        |                       ^   ^
        |                       |   |
        |                 Large Transactions
        |                       |
        |                       |
        |                       v
        |                 ENRICHED DATA
        |
        |
        |Example final record:
        |
        |T000000001
        |A000001
        |BR001
        |DEPOSIT
        |100
        |2026-09-01
        |Delhi Main Branch
        |Delhi
        |Delhi
        |North
        |Rajesh Kumar
        |
        |
        |The transaction originally only knows branch_id.
        |
        |The broadcast join enriches it with branch information.
        |"""
        .stripMargin
    )


    // ============================================================
    // 23. VIVA SUMMARY
    // ============================================================

    println()
    println("=" * 90)
    println("22. DAY 19 VIVA SUMMARY")
    println("=" * 90)

    println(
      """
        |Q1. What is a broadcast join?
        |
        |A:
        |A broadcast join distributes a small dataset to the
        |executors so it can be joined locally with a larger
        |dataset without shuffling the large dataset.
        |
        |
        |Q2. Why is broadcast join useful?
        |
        |A:
        |It can avoid a large shuffle when one side of the
        |join is sufficiently small.
        |
        |
        |Q3. What function explicitly requests broadcasting?
        |
        |A:
        |
        |broadcast()
        |
        |Example:
        |
        |transactions.join(
        |    broadcast(branches),
        |    ...
        |)
        |
        |
        |Q4. What should be small for a broadcast join?
        |
        |A:
        |The side being broadcast should be small enough to fit
        |comfortably in executor memory.
        |
        |
        |Q5. What is Shuffle Sort Merge Join?
        |
        |A:
        |It is a join strategy where data is shuffled according
        |to the join key, sorted within partitions and then merged.
        |
        |
        |Q6. What is the major difference?
        |
        |A:
        |
        |Broadcast Join:
        |Small side is broadcast to executors.
        |
        |Shuffle Sort Merge Join:
        |Both sides are generally repartitioned/shuffled and sorted.
        |
        |
        |Q7. Why can shuffle be expensive?
        |
        |A:
        |It may require network transfer, serialization,
        |disk I/O and sorting.
        |
        |
        |Q8. Does row count alone determine whether a table
        |should be broadcast?
        |
        |A:
        |No. The actual size of the serialized dataset and
        |available executor memory matter.
        |
        |
        |Q9. What happens if we broadcast a very large table?
        |
        |A:
        |It can create substantial memory pressure on executors
        |and can hurt performance or cause failures.
        |
        |
        |Q10. What is the banking use case in this project?
        |
        |A:
        |Millions of transactions are joined with a small branch
        |master using branch_id. The branch master can be broadcast
        |to enrich every transaction with branch information.
        |"""
        .stripMargin
    )


    // ============================================================
    // 24. FINAL SUMMARY
    // ============================================================

    println()
    println("=" * 90)
    println("DAY 19 COMPLETED")
    println("=" * 90)

    println(
      """
        |Topics completed:
        |
        |[✓] Large transaction fact DataFrame
        |[✓] Small branch reference DataFrame
        |[✓] Normal join
        |[✓] Explicit broadcast join
        |[✓] Broadcast execution plan
        |[✓] BroadcastHashJoin
        |[✓] BroadcastExchange
        |[✓] SQL broadcast hint
        |[✓] Transaction analytics
        |[✓] Shuffle Sort Merge Join comparison
        |[✓] When to use broadcast
        |[✓] When not to use broadcast
        |[✓] Real-world banking scenario
        |[✓] Day 19 viva questions
        |"""
        .stripMargin
    )

    println()

    spark.stop()
  }
}
