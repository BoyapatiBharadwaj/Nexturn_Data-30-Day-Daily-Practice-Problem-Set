import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Day22BatchPipelineApp {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // 1. Create SparkSession
    // ------------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day22 E-Commerce Batch Pipeline")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n==============================================")
    println("DAY 22 - E-COMMERCE BATCH PIPELINE")
    println("==============================================")

    // ------------------------------------------------------------
    // 2. Input paths
    // ------------------------------------------------------------

    val transactionsPath = "data/transactions.csv"
    val customersPath = "data/customers.csv"
    val productsPath = "data/products.csv"

    // ------------------------------------------------------------
    // 3. Output paths
    // ------------------------------------------------------------

    val cleanedOutput =
      "output/cleaned_transactions"

    val enrichedOutput =
      "output/enriched_transactions"

    val dailySalesOutput =
      "output/daily_sales"

    val productSalesOutput =
      "output/product_sales"

    val categorySalesOutput =
      "output/category_sales"

    // ------------------------------------------------------------
    // 4. Read raw transactions
    // ------------------------------------------------------------

    println("\n========== RAW TRANSACTIONS ==========")

    val rawTransactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(transactionsPath)

    rawTransactions.show(false)

    println(s"Raw transaction count = ${rawTransactions.count()}")

    rawTransactions.printSchema()

    // ------------------------------------------------------------
    // 5. Read customer data
    // ------------------------------------------------------------

    println("\n========== CUSTOMERS ==========")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(customersPath)

    customers.show(false)

    println(s"Customer count = ${customers.count()}")

    // ------------------------------------------------------------
    // 6. Read product data
    // ------------------------------------------------------------

    println("\n========== PRODUCTS ==========")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(productsPath)

    products.show(false)

    println(s"Product count = ${products.count()}")

    // ------------------------------------------------------------
    // 7. Data quality checks
    // ------------------------------------------------------------

    println("\n========== DATA QUALITY CHECKS ==========")

    val invalidQuantity =
      rawTransactions.filter(
        col("quantity").isNull ||
        col("quantity") <= 0
      )

    println("\nInvalid quantity records:")
    invalidQuantity.show(false)

    println(
      s"Invalid quantity count = ${invalidQuantity.count()}"
    )

    val cancelledTransactions =
      rawTransactions.filter(
        upper(trim(col("status"))) === "CANCELLED"
      )

    println("\nCancelled transactions:")
    cancelledTransactions.show(false)

    println(
      s"Cancelled count = ${cancelledTransactions.count()}"
    )

    // ------------------------------------------------------------
    // 8. Clean transactions
    // ------------------------------------------------------------

    println("\n========== CLEAN TRANSACTIONS ==========")

    val cleanedTransactions = rawTransactions
      .withColumn(
        "status",
        upper(trim(col("status")))
      )
      .filter(col("transaction_id").isNotNull)
      .filter(col("customer_id").isNotNull)
      .filter(col("product_id").isNotNull)
      .filter(col("quantity").isNotNull)
      .filter(col("quantity") > 0)
      .filter(col("transaction_date").isNotNull)
      .filter(col("status") === "COMPLETED")

    cleanedTransactions.show(false)

    println(
      s"Clean transaction count = ${cleanedTransactions.count()}"
    )

    println(
      s"Records removed = ${rawTransactions.count() - cleanedTransactions.count()}"
    )

    // ------------------------------------------------------------
    // 9. Write cleaned transactions
    // ------------------------------------------------------------

    println("\n========== WRITE CLEANED DATA ==========")

    cleanedTransactions
      .write
      .mode("overwrite")
      .parquet(cleanedOutput)

    println(
      s"Cleaned transactions written to: $cleanedOutput"
    )

    // ------------------------------------------------------------
    // 10. Join transactions with customers
    // ------------------------------------------------------------

    println("\n========== CUSTOMER JOIN ==========")

    val customerEnriched = cleanedTransactions
      .alias("t")
      .join(
        customers.alias("c"),
        col("t.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("t.transaction_id"),
        col("t.customer_id"),
        col("t.product_id"),
        col("t.quantity"),
        col("t.transaction_date"),
        col("t.status"),
        col("c.customer_name"),
        col("c.city"),
        col("c.customer_segment")
      )

    customerEnriched.show(false)

    println(
      s"Customer-enriched count = ${customerEnriched.count()}"
    )

    // ------------------------------------------------------------
    // 11. Join with products
    // ------------------------------------------------------------

    println("\n========== PRODUCT JOIN ==========")

    val enrichedTransactions = customerEnriched
      .alias("t")
      .join(
        products.alias("p"),
        col("t.product_id") === col("p.product_id"),
        "inner"
      )
      .select(
        col("t.transaction_id"),
        col("t.customer_id"),
        col("t.customer_name"),
        col("t.city"),
        col("t.customer_segment"),
        col("t.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.unit_price"),
        col("t.quantity"),
        col("t.transaction_date"),
        col("t.status")
      )
      .withColumn(
        "revenue",
        col("quantity") * col("unit_price")
      )
      .withColumn(
        "year",
        year(col("transaction_date"))
      )
      .withColumn(
        "month",
        month(col("transaction_date"))
      )
      .withColumn(
        "day",
        dayofmonth(col("transaction_date"))
      )

    println("\nFinal enriched transaction dataset:")
    enrichedTransactions.show(false)

    enrichedTransactions.printSchema()

    println(
      s"Enriched transaction count = ${enrichedTransactions.count()}"
    )

    // ------------------------------------------------------------
    // 12. Write enriched transactions
    // ------------------------------------------------------------

    enrichedTransactions
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(enrichedOutput)

    println(
      s"Enriched transactions written to: $enrichedOutput"
    )

    // ------------------------------------------------------------
    // 13. Daily revenue aggregation
    // ------------------------------------------------------------

    println("\n========== DAILY SALES ==========")

    val dailySales = enrichedTransactions
      .groupBy(
        col("transaction_date"),
        col("year"),
        col("month"),
        col("day")
      )
      .agg(
        countDistinct("transaction_id")
          .alias("transaction_count"),

        sum("quantity")
          .alias("units_sold"),

        sum("revenue")
          .alias("total_revenue"),

        avg("revenue")
          .alias("average_transaction_value")
      )
      .orderBy(col("transaction_date"))

    dailySales.show(false)

    // ------------------------------------------------------------
    // 14. Write daily sales partitioned by date
    // ------------------------------------------------------------

    dailySales
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(dailySalesOutput)

    println(
      s"Daily sales written to: $dailySalesOutput"
    )

    // ------------------------------------------------------------
    // 15. Product-level sales
    // ------------------------------------------------------------

    println("\n========== PRODUCT SALES ==========")

    val productSales = enrichedTransactions
      .groupBy(
        col("product_id"),
        col("product_name"),
        col("category")
      )
      .agg(
        sum("quantity")
          .alias("units_sold"),

        sum("revenue")
          .alias("total_revenue"),

        countDistinct("transaction_id")
          .alias("transaction_count")
      )
      .orderBy(col("total_revenue").desc)

    productSales.show(false)

    productSales
      .write
      .mode("overwrite")
      .parquet(productSalesOutput)

    println(
      s"Product sales written to: $productSalesOutput"
    )

    // ------------------------------------------------------------
    // 16. Category-level sales
    // ------------------------------------------------------------

    println("\n========== CATEGORY SALES ==========")

    val categorySales = enrichedTransactions
      .groupBy(col("category"))
      .agg(
        sum("quantity")
          .alias("units_sold"),

        sum("revenue")
          .alias("total_revenue"),

        countDistinct("transaction_id")
          .alias("transaction_count")
      )
      .orderBy(col("total_revenue").desc)

    categorySales.show(false)

    categorySales
      .write
      .mode("overwrite")
      .parquet(categorySalesOutput)

    println(
      s"Category sales written to: $categorySalesOutput"
    )

    // ------------------------------------------------------------
    // 17. Customer-level revenue
    // ------------------------------------------------------------

    println("\n========== CUSTOMER REVENUE ==========")

    val customerSales = enrichedTransactions
      .groupBy(
        col("customer_id"),
        col("customer_name"),
        col("city"),
        col("customer_segment")
      )
      .agg(
        sum("revenue")
          .alias("total_revenue"),

        sum("quantity")
          .alias("units_purchased"),

        countDistinct("transaction_id")
          .alias("transaction_count")
      )
      .orderBy(col("total_revenue").desc)

    customerSales.show(false)

    // ------------------------------------------------------------
    // 18. High-value transactions
    // ------------------------------------------------------------

    println("\n========== HIGH-VALUE TRANSACTIONS ==========")

    val highValueTransactions =
      enrichedTransactions
        .filter(col("revenue") >= 50000)
        .orderBy(col("revenue").desc)

    highValueTransactions.show(false)

    // ------------------------------------------------------------
    // 19. Partition pruning demonstration
    // ------------------------------------------------------------

    println("\n========== PARTITION PRUNING ==========")

    val september9Sales = spark.read
      .parquet(dailySalesOutput)
      .filter(
        col("year") === 2026 &&
        col("month") === 9 &&
        col("day") === 9
      )

    september9Sales.show(false)

    println(
      "\nPhysical plan for partition pruning:"
    )

    september9Sales.explain(true)

    // ------------------------------------------------------------
    // 20. Revenue by city
    // ------------------------------------------------------------

    println("\n========== CITY REVENUE ==========")

    val cityRevenue = enrichedTransactions
      .groupBy("city")
      .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("units_sold")
      )
      .orderBy(col("total_revenue").desc)

    cityRevenue.show(false)

    // ------------------------------------------------------------
    // 21. Final pipeline summary
    // ------------------------------------------------------------

    println("\n==============================================")
    println("PIPELINE COMPLETED")
    println("==============================================")

    println(
      s"""
         |Raw transactions       : ${rawTransactions.count()}
         |Clean transactions     : ${cleanedTransactions.count()}
         |Removed records        : ${rawTransactions.count() - cleanedTransactions.count()}
         |
         |Output locations:
         |  Cleaned transactions : $cleanedOutput
         |  Enriched transactions: $enrichedOutput
         |  Daily sales          : $dailySalesOutput
         |  Product sales        : $productSalesOutput
         |  Category sales       : $categorySalesOutput
         |""".stripMargin
    )

    // ------------------------------------------------------------
    // 22. Stop Spark
    // ------------------------------------------------------------

    spark.stop()
  }
}
