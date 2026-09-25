import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day18JoinsApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Day18 - Spark Joins")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("=" * 80)
    println("DAY 18 - SPARK JOINS")
    println("=" * 80)
    println()


    // ============================================================
    // 2. READ CUSTOMERS
    // ============================================================

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("=" * 80)
    println("1. CUSTOMERS")
    println("=" * 80)

    customers.show(20, false)

    customers.printSchema()


    // ============================================================
    // 3. READ ORDERS
    // ============================================================

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    println()
    println("=" * 80)
    println("2. ORDERS")
    println("=" * 80)

    orders.show(20, false)

    orders.printSchema()


    // ============================================================
    // 4. READ PAYMENTS
    // ============================================================

    val payments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/payments.csv")

    println()
    println("=" * 80)
    println("3. PAYMENTS")
    println("=" * 80)

    payments.show(20, false)

    payments.printSchema()


    // ============================================================
    // 5. INNER JOIN
    // ============================================================

    println()
    println("=" * 80)
    println("4. INNER JOIN - ORDERS + CUSTOMERS")
    println("=" * 80)

    val innerJoin =
      orders.join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "inner"
      )

    innerJoin
      .select(
        orders("order_id"),
        orders("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("product"),
        orders("order_amount")
      )
      .orderBy(col("order_id"))
      .show(100, false)

    println(
      """
        |INNER JOIN:
        |
        |Only rows having a matching key in BOTH datasets
        |are returned.
        |
        |orders.customer_id = customers.customer_id
        |"""
        .stripMargin
    )


    // ============================================================
    // 6. LEFT JOIN
    // ============================================================

    println()
    println("=" * 80)
    println("5. LEFT JOIN - ORDERS + CUSTOMERS")
    println("=" * 80)

    val leftJoin =
      orders.join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "left"
      )

    leftJoin
      .select(
        orders("order_id"),
        orders("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("product"),
        orders("order_amount")
      )
      .orderBy(col("order_id"))
      .show(100, false)

    println(
      """
        |LEFT JOIN:
        |
        |Keeps ALL rows from the LEFT table.
        |
        |If there is no matching customer,
        |customer columns become NULL.
        |"""
        .stripMargin
    )


    // ============================================================
    // 7. RIGHT JOIN
    // ============================================================

    println()
    println("=" * 80)
    println("6. RIGHT JOIN - ORDERS + CUSTOMERS")
    println("=" * 80)

    val rightJoin =
      orders.join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "right"
      )

    rightJoin
      .select(
        orders("order_id"),
        customers("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("product"),
        orders("order_amount")
      )
      .orderBy(col("customer_id"))
      .show(100, false)

    println(
      """
        |RIGHT JOIN:
        |
        |Keeps ALL rows from the RIGHT table.
        |
        |Customers without orders will have NULL
        |in the order columns.
        |"""
        .stripMargin
    )


    // ============================================================
    // 8. FULL OUTER JOIN
    // ============================================================

    println()
    println("=" * 80)
    println("7. FULL OUTER JOIN - ORDERS + CUSTOMERS")
    println("=" * 80)

    val fullJoin =
      orders.join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "full"
      )

    fullJoin
      .select(
        coalesce(
          orders("customer_id"),
          customers("customer_id")
        ).alias("customer_id"),
        orders("order_id"),
        customers("customer_name"),
        customers("city"),
        orders("product"),
        orders("order_amount")
      )
      .orderBy(col("customer_id"))
      .show(100, false)

    println(
      """
        |FULL OUTER JOIN:
        |
        |Keeps ALL rows from BOTH datasets.
        |
        |Matched rows are combined.
        |
        |Unmatched rows contain NULL on the missing side.
        |"""
        .stripMargin
    )


    // ============================================================
    // 9. AMBIGUOUS COLUMN NAMES
    // ============================================================

    println()
    println("=" * 80)
    println("8. HANDLING AMBIGUOUS COLUMN NAMES")
    println("=" * 80)

    println(
      """
        |Both orders and customers contain:
        |
        |customer_id
        |
        |If we reference:
        |
        |col("customer_id")
        |
        |after a join, Spark may report an ambiguous
        |reference because both DataFrames contain that column.
        |
        |Solution:
        |
        |Use aliases.
        |"""
        .stripMargin
    )


    // ============================================================
    // 10. ALIAS DATAFRAMES
    // ============================================================

    val o = orders.alias("o")
    val c = customers.alias("c")

    val aliasedJoin =
      o.join(
        c,
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )

    aliasedJoin
      .select(
        col("o.order_id").alias("order_id"),
        col("o.customer_id").alias("order_customer_id"),
        col("c.customer_id").alias("customer_customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.order_amount")
      )
      .orderBy(col("o.order_id"))
      .show(100, false)


    // ============================================================
    // 11. LEFT JOIN + NULL HANDLING
    // ============================================================

    println()
    println("=" * 80)
    println("9. NULL HANDLING AFTER LEFT JOIN")
    println("=" * 80)

    val ordersWithCustomers =
      orders.alias("o")
        .join(
          customers.alias("c"),
          col("o.customer_id") === col("c.customer_id"),
          "left"
        )

    val nullHandledOrders =
      ordersWithCustomers
        .select(
          col("o.order_id"),
          col("o.customer_id"),
          coalesce(
            col("c.customer_name"),
            lit("Unknown Customer")
          ).alias("customer_name"),
          coalesce(
            col("c.city"),
            lit("Unknown City")
          ).alias("city"),
          col("o.product"),
          col("o.order_amount")
        )
        .orderBy(col("order_id"))

    nullHandledOrders.show(100, false)


    // ============================================================
    // 12. FIND ORDERS WITH MISSING CUSTOMERS
    // ============================================================

    println()
    println("=" * 80)
    println("10. ORDERS WITH NO CUSTOMER MATCH")
    println("=" * 80)

    val unmatchedOrders =
      orders.alias("o")
        .join(
          customers.alias("c"),
          col("o.customer_id") === col("c.customer_id"),
          "left"
        )
        .filter(
          col("c.customer_id").isNull
        )
        .select(
          col("o.order_id"),
          col("o.customer_id"),
          col("o.product"),
          col("o.order_amount")
        )

    unmatchedOrders.show(100, false)


    // ============================================================
    // 13. CUSTOMERS WITHOUT ORDERS
    // ============================================================

    println()
    println("=" * 80)
    println("11. CUSTOMERS WITH NO ORDERS")
    println("=" * 80)

    val customersWithoutOrders =
      customers.alias("c")
        .join(
          orders.alias("o"),
          col("c.customer_id") === col("o.customer_id"),
          "left"
        )
        .filter(
          col("o.order_id").isNull
        )
        .select(
          col("c.customer_id"),
          col("c.customer_name"),
          col("c.city"),
          col("c.segment")
        )

    customersWithoutOrders.show(100, false)


    // ============================================================
    // 14. JOIN ORDERS + PAYMENTS
    // ============================================================

    println()
    println("=" * 80)
    println("12. ORDERS + PAYMENTS")
    println("=" * 80)

    val ordersPayments =
      orders.alias("o")
        .join(
          payments.alias("p"),
          col("o.order_id") === col("p.order_id"),
          "left"
        )

    ordersPayments
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product"),
        col("o.order_amount"),
        col("p.payment_method"),
        col("p.payment_status"),
        col("p.payment_amount")
      )
      .orderBy(col("o.order_id"))
      .show(100, false)


    // ============================================================
    // 15. THREE-WAY JOIN
    // ORDERS + CUSTOMERS + PAYMENTS
    // ============================================================

    println()
    println("=" * 80)
    println("13. THREE-WAY JOIN")
    println("ORDERS + CUSTOMERS + PAYMENTS")
    println("=" * 80)

    val threeWayJoin =
      orders.alias("o")
        .join(
          customers.alias("c"),
          col("o.customer_id") === col("c.customer_id"),
          "left"
        )
        .join(
          payments.alias("p"),
          col("o.order_id") === col("p.order_id"),
          "left"
        )

    val finalOrderReport =
      threeWayJoin
        .select(
          col("o.order_id"),
          col("o.customer_id"),
          col("c.customer_name"),
          col("c.city"),
          col("c.segment"),
          col("o.product"),
          col("o.quantity"),
          col("o.order_amount"),
          coalesce(
            col("p.payment_method"),
            lit("No Payment")
          ).alias("payment_method"),
          coalesce(
            col("p.payment_status"),
            lit("No Payment")
          ).alias("payment_status"),
          coalesce(
            col("p.payment_amount"),
            lit(0)
          ).alias("payment_amount")
        )
        .orderBy(col("o.order_id"))

    finalOrderReport.show(100, false)


    // ============================================================
    // 16. PAYMENT VALIDATION
    // ============================================================

    println()
    println("=" * 80)
    println("14. PAYMENT VALIDATION")
    println("=" * 80)

    val paymentValidation =
      threeWayJoin
        .select(
          col("o.order_id"),
          col("o.order_amount"),
          coalesce(
            col("p.payment_amount"),
            lit(0)
          ).alias("payment_amount")
        )
        .withColumn(
          "payment_difference",
          col("order_amount") - col("payment_amount")
        )
        .withColumn(
          "payment_check",
          when(
            col("payment_amount") === 0,
            "No Payment"
          )
            .when(
              col("order_amount") === col("payment_amount"),
              "Fully Paid"
            )
            .when(
              col("payment_amount") < col("order_amount"),
              "Partially Paid"
            )
            .otherwise("Overpaid")
        )
        .orderBy(col("order_id"))

    paymentValidation.show(100, false)


    // ============================================================
    // 17. CUSTOMER REVENUE
    // ============================================================

    println()
    println("=" * 80)
    println("15. CUSTOMER REVENUE")
    println("=" * 80)

    val customerRevenue =
      orders.alias("o")
        .join(
          customers.alias("c"),
          col("o.customer_id") === col("c.customer_id"),
          "inner"
        )
        .groupBy(
          col("c.customer_id"),
          col("c.customer_name"),
          col("c.city"),
          col("c.segment")
        )
        .agg(
          count(col("o.order_id")).alias("order_count"),
          sum(col("o.order_amount")).alias("total_revenue"),
          avg(col("o.order_amount")).alias("average_order_value")
        )
        .orderBy(
          col("total_revenue").desc
        )

    customerRevenue.show(100, false)


    // ============================================================
    // 18. PAYMENT STATUS ANALYSIS
    // ============================================================

    println()
    println("=" * 80)
    println("16. PAYMENT STATUS ANALYSIS")
    println("=" * 80)

    val paymentStatusAnalysis =
      threeWayJoin
        .groupBy(
          coalesce(
            col("p.payment_status"),
            lit("No Payment")
          ).alias("payment_status")
        )
        .agg(
          count(col("o.order_id")).alias("order_count"),
          sum(col("o.order_amount")).alias("order_value"),
          sum(
            coalesce(
              col("p.payment_amount"),
              lit(0)
            )
          ).alias("payment_value")
        )
        .orderBy(
          col("order_value").desc
        )

    paymentStatusAnalysis.show(100, false)


    // ============================================================
    // 19. FULL JOIN ORDERS + PAYMENTS
    // ============================================================

    println()
    println("=" * 80)
    println("17. FULL JOIN - ORDERS + PAYMENTS")
    println("=" * 80)

    val fullOrderPaymentJoin =
      orders.alias("o")
        .join(
          payments.alias("p"),
          col("o.order_id") === col("p.order_id"),
          "full"
        )

    fullOrderPaymentJoin
      .select(
        coalesce(
          col("o.order_id"),
          col("p.order_id")
        ).alias("order_id"),
        col("o.customer_id"),
        col("o.product"),
        col("o.order_amount"),
        col("p.payment_id"),
        col("p.payment_method"),
        col("p.payment_status"),
        col("p.payment_amount")
      )
      .orderBy(col("order_id"))
      .show(100, false)


    // ============================================================
    // 20. FIND ORPHAN PAYMENTS
    // ============================================================

    println()
    println("=" * 80)
    println("18. ORPHAN PAYMENTS")
    println("=" * 80)

    val orphanPayments =
      payments.alias("p")
        .join(
          orders.alias("o"),
          col("p.order_id") === col("o.order_id"),
          "left"
        )
        .filter(
          col("o.order_id").isNull
        )
        .select(
          col("p.payment_id"),
          col("p.order_id"),
          col("p.payment_method"),
          col("p.payment_status"),
          col("p.payment_amount")
        )

    orphanPayments.show(100, false)


    // ============================================================
    // 21. EXPLAIN JOIN EXECUTION PLAN
    // ============================================================

    println()
    println("=" * 80)
    println("19. JOIN EXECUTION PLAN")
    println("=" * 80)

    val explainJoin =
      orders.alias("o")
        .join(
          customers.alias("c"),
          col("o.customer_id") === col("c.customer_id"),
          "inner"
        )

    explainJoin.explain(true)


    // ============================================================
    // 22. SHUFFLE SORT MERGE JOIN
    // ============================================================

    println()
    println("=" * 80)
    println("20. SHUFFLE SORT MERGE JOIN")
    println("=" * 80)

    println(
      """
        |Shuffle Sort Merge Join
        |
        |A common Spark join strategy for joining relatively
        |large datasets.
        |
        |Conceptually:
        |
        |             ORDERS
        |                |
        |                v
        |          Hash Partition
        |                |
        |                v
        |             Shuffle
        |                |
        |                v
        |               Sort
        |                |
        |                |
        |                +----------+
        |                           |
        |                           v
        |                       MERGE JOIN
        |                           ^
        |                           |
        |                +----------+
        |                |
        |                v
        |             Sort
        |                |
        |                v
        |             Shuffle
        |                ^
        |                |
        |          Hash Partition
        |                ^
        |                |
        |            CUSTOMERS
        |
        |
        |Typical process:
        |
        |1. Partition both datasets by join key.
        |
        |2. Shuffle data across the cluster.
        |
        |3. Sort rows within each partition.
        |
        |4. Merge matching sorted records.
        |
        |
        |Why is it called:
        |
        |Shuffle -> data is redistributed
        |
        |Sort -> each partition is sorted
        |
        |Merge -> matching sorted records are merged
        |"""
        .stripMargin
    )


    // ============================================================
    // 23. JOIN TYPES SUMMARY
    // ============================================================

    println()
    println("=" * 80)
    println("21. JOIN TYPES SUMMARY")
    println("=" * 80)

    println(
      """
        |INNER JOIN
        |-----------
        |Returns only matching rows.
        |
        |
        |LEFT JOIN
        |---------
        |All rows from left table.
        |Unmatched right columns become NULL.
        |
        |
        |RIGHT JOIN
        |----------
        |All rows from right table.
        |Unmatched left columns become NULL.
        |
        |
        |FULL OUTER JOIN
        |---------------
        |All rows from both tables.
        |Missing side becomes NULL.
        |"""
        .stripMargin
    )


    // ============================================================
    // 24. DAY 18 VIVA QUESTIONS
    // ============================================================

    println()
    println("=" * 80)
    println("22. DAY 18 VIVA SUMMARY")
    println("=" * 80)

    println(
      """
        |Q1. What is a join?
        |
        |A:
        |A join combines rows from two datasets based on
        |a related column or condition.
        |
        |
        |Q2. What is an inner join?
        |
        |A:
        |It returns only rows having matching keys in both datasets.
        |
        |
        |Q3. What is a left join?
        |
        |A:
        |It keeps every row from the left dataset.
        |If there is no match on the right side, right-side
        |columns become NULL.
        |
        |
        |Q4. What is a right join?
        |
        |A:
        |It keeps every row from the right dataset.
        |
        |
        |Q5. What is a full outer join?
        |
        |A:
        |It keeps rows from both datasets, including unmatched rows.
        |
        |
        |Q6. Why do we use aliases?
        |
        |A:
        |Aliases distinguish columns from different DataFrames
        |when they have the same column name.
        |
        |
        |Example:
        |
        |orders.alias("o")
        |customers.alias("c")
        |
        |col("o.customer_id")
        |col("c.customer_id")
        |
        |
        |Q7. How do you handle NULL after a left join?
        |
        |A:
        |Use functions such as:
        |
        |coalesce()
        |when()
        |isNull()
        |na.fill()
        |
        |
        |Q8. What is Shuffle Sort Merge Join?
        |
        |A:
        |It is a Spark join strategy where both datasets are
        |partitioned/shuffled according to the join key,
        |sorted within partitions, and then matching rows
        |are merged.
        |
        |
        |Q9. Why does shuffle happen during a join?
        |
        |A:
        |Rows having the same join key may initially exist
        |in different partitions. Spark redistributes them
        |so matching keys can be brought together.
        |
        |
        |Q10. Why is shuffle expensive?
        |
        |A:
        |It can involve network transfer, disk I/O and sorting,
        |which adds execution overhead.
        |
        |
        |Q11. What is a broadcast join?
        |
        |A:
        |Spark can broadcast a small dataset to executors so
        |the larger dataset does not need a full shuffle for
        |that join.
        |
        |
        |Q12. When might Spark use a broadcast join?
        |
        |A:
        |When one side of the join is sufficiently small and
        |Spark's optimizer determines that broadcasting it is
        |appropriate.
        |"""
        .stripMargin
    )


    // ============================================================
    // 25. FINAL PROJECT FLOW
    // ============================================================

    println()
    println("=" * 80)
    println("23. FINAL PROJECT FLOW")
    println("=" * 80)

    println(
      """
        |                    CUSTOMERS
        |                        |
        |                        |
        |                        | customer_id
        |                        |
        |                        v
        |ORDERS ---------------- JOIN
        |  |
        |  |
        |  | order_id
        |  |
        |  v
        |PAYMENTS
        |
        |
        |Final analytical dataset:
        |
        |Order
        |  +
        |Customer
        |  +
        |Payment
        |
        |        |
        |        v
        |
        |Customer analytics
        |Payment validation
        |Revenue analysis
        |Missing customer detection
        |Orphan payment detection
        |"""
        .stripMargin
    )


    // ============================================================
    // 26. STOP SPARK
    // ============================================================

    println()
    println("=" * 80)
    println("DAY 18 COMPLETED")
    println("=" * 80)
    println()

    spark.stop()
  }
}
