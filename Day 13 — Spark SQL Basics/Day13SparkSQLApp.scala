import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Day13SparkSQLApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. CREATE SPARK SESSION
    // ============================================================

    val cores =
      if (args.nonEmpty) args(0)
      else "4"

    val master = s"local[$cores]"

    val spark = SparkSession
      .builder()
      .appName("Day 13 - Spark SQL Basics")
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("============================================================")
    println("DAY 13 - SPARK SQL BASICS")
    println("============================================================")
    println(s"Spark Master       : $master")
    println(
      s"Default Parallelism: ${spark.sparkContext.defaultParallelism}"
    )
    println("============================================================")


    // ============================================================
    // 2. READ CSV INTO DATAFRAME
    // ============================================================

    println()
    println("2. READ CSV DATA")
    println("------------------------------------------------------------")

    val csvPath = "data/customers.csv"

    val customersCSV: DataFrame =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvPath)

    println("CSV DataFrame created successfully.")

    customersCSV.show(
      numRows = 20,
      truncate = false
    )


    // ============================================================
    // 3. INSPECT CSV SCHEMA
    // ============================================================

    println()
    println("3. CSV SCHEMA")
    println("------------------------------------------------------------")

    customersCSV.printSchema()


    // ============================================================
    // 4. SELECT COLUMNS
    // ============================================================

    println()
    println("4. SELECT COLUMNS")
    println("------------------------------------------------------------")

    val selectedCustomers =
      customersCSV.select(
        "customer_id",
        "name",
        "city",
        "purchase_amount"
      )

    selectedCustomers.show(
      truncate = false
    )


    // ============================================================
    // 5. FILTER CUSTOMERS
    // ============================================================

    println()
    println("5. FILTER CUSTOMERS")
    println("------------------------------------------------------------")

    println("Customers with purchase_amount > 50000:")

    val highValueCustomers =
      customersCSV
        .filter(col("purchase_amount") > 50000)

    highValueCustomers.show(
      truncate = false
    )


    // ============================================================
    // 6. MULTIPLE FILTER CONDITIONS
    // ============================================================

    println()
    println("6. MULTIPLE FILTER CONDITIONS")
    println("------------------------------------------------------------")

    println(
      "Active customers with purchase_amount > 50000:"
    )

    val activeHighValueCustomers =
      customersCSV
        .filter(
          col("status") === "Active" &&
          col("purchase_amount") > 50000
        )

    activeHighValueCustomers.show(
      truncate = false
    )


    // ============================================================
    // 7. WITHCOLUMN - CREATE NEW COLUMN
    // ============================================================

    println()
    println("7. WITHCOLUMN")
    println("------------------------------------------------------------")

    /*
     * Create average purchase value.
     *
     * Formula:
     *
     * purchase_amount / purchase_count
     */

    val customersWithAverage =
      customersCSV.withColumn(
        "average_purchase",
        round(
          col("purchase_amount") /
            col("purchase_count"),
          2
        )
      )

    customersWithAverage.show(
      truncate = false
    )


    // ============================================================
    // 8. WITHCOLUMN - CUSTOMER SEGMENT
    // ============================================================

    println()
    println("8. CUSTOMER SEGMENT")
    println("------------------------------------------------------------")

    /*
     * Customer segmentation:
     *
     * purchase_amount >= 70000
     *      -> Premium
     *
     * purchase_amount >= 50000
     *      -> Gold
     *
     * otherwise
     *      -> Standard
     */

    val customersWithSegment =
      customersWithAverage.withColumn(
        "customer_segment",
        when(
          col("purchase_amount") >= 70000,
          "Premium"
        )
          .when(
            col("purchase_amount") >= 50000,
            "Gold"
          )
          .otherwise("Standard")
      )

    customersWithSegment.show(
      truncate = false
    )


    // ============================================================
    // 9. TEMPORARY VIEW
    // ============================================================

    println()
    println("9. CREATE TEMPORARY VIEW")
    println("------------------------------------------------------------")

    customersWithSegment
      .createOrReplaceTempView("customers")

    println(
      "Temporary view 'customers' created successfully."
    )


    // ============================================================
    // 10. BASIC SPARK SQL QUERY
    // ============================================================

    println()
    println("10. BASIC SPARK SQL")
    println("------------------------------------------------------------")

    val sqlAllCustomers =
      spark.sql(
        """
          SELECT
            customer_id,
            name,
            age,
            city,
            department,
            purchase_amount,
            purchase_count,
            status,
            average_purchase,
            customer_segment
          FROM customers
        """
      )

    sqlAllCustomers.show(
      truncate = false
    )


    // ============================================================
    // 11. SQL FILTER
    // ============================================================

    println()
    println("11. SQL FILTER")
    println("------------------------------------------------------------")

    println(
      "Active customers with purchase amount above ₹50000:"
    )

    val sqlHighValue =
      spark.sql(
        """
          SELECT
            customer_id,
            name,
            city,
            purchase_amount,
            customer_segment
          FROM customers
          WHERE status = 'Active'
            AND purchase_amount > 50000
          ORDER BY purchase_amount DESC
        """
      )

    sqlHighValue.show(
      truncate = false
    )


    // ============================================================
    // 12. CUSTOMER ANALYTICS - TOTAL CUSTOMERS
    // ============================================================

    println()
    println("12. TOTAL CUSTOMERS")
    println("------------------------------------------------------------")

    val totalCustomers =
      spark.sql(
        """
          SELECT COUNT(*) AS total_customers
          FROM customers
        """
      )

    totalCustomers.show()


    // ============================================================
    // 13. CUSTOMER ANALYTICS - ACTIVE CUSTOMERS
    // ============================================================

    println()
    println("13. ACTIVE CUSTOMERS")
    println("------------------------------------------------------------")

    val activeCustomers =
      spark.sql(
        """
          SELECT COUNT(*) AS active_customers
          FROM customers
          WHERE status = 'Active'
        """
      )

    activeCustomers.show()


    // ============================================================
    // 14. CUSTOMER ANALYTICS - TOTAL REVENUE
    // ============================================================

    println()
    println("14. TOTAL PURCHASE AMOUNT")
    println("------------------------------------------------------------")

    val totalRevenue =
      spark.sql(
        """
          SELECT
            ROUND(SUM(purchase_amount), 2) AS total_purchase_amount
          FROM customers
        """
      )

    totalRevenue.show()


    // ============================================================
    // 15. REVENUE BY CITY
    // ============================================================

    println()
    println("15. REVENUE BY CITY")
    println("------------------------------------------------------------")

    val revenueByCity =
      spark.sql(
        """
          SELECT
            city,
            COUNT(*) AS customer_count,
            ROUND(SUM(purchase_amount), 2) AS total_purchase_amount,
            ROUND(AVG(purchase_amount), 2) AS average_purchase_amount
          FROM customers
          GROUP BY city
          ORDER BY total_purchase_amount DESC
        """
      )

    revenueByCity.show(
      truncate = false
    )


    // ============================================================
    // 16. REVENUE BY DEPARTMENT
    // ============================================================

    println()
    println("16. REVENUE BY DEPARTMENT")
    println("------------------------------------------------------------")

    val revenueByDepartment =
      spark.sql(
        """
          SELECT
            department,
            COUNT(*) AS customer_count,
            ROUND(SUM(purchase_amount), 2) AS total_purchase_amount,
            ROUND(AVG(purchase_amount), 2) AS average_purchase_amount
          FROM customers
          GROUP BY department
          ORDER BY total_purchase_amount DESC
        """
      )

    revenueByDepartment.show(
      truncate = false
    )


    // ============================================================
    // 17. CUSTOMER SEGMENT ANALYSIS
    // ============================================================

    println()
    println("17. CUSTOMER SEGMENT ANALYSIS")
    println("------------------------------------------------------------")

    val segmentAnalysis =
      spark.sql(
        """
          SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            ROUND(SUM(purchase_amount), 2) AS total_purchase_amount,
            ROUND(AVG(purchase_amount), 2) AS average_purchase_amount
          FROM customers
          GROUP BY customer_segment
          ORDER BY total_purchase_amount DESC
        """
      )

    segmentAnalysis.show(
      truncate = false
    )


    // ============================================================
    // 18. TOP 5 CUSTOMERS
    // ============================================================

    println()
    println("18. TOP 5 CUSTOMERS")
    println("------------------------------------------------------------")

    val topCustomers =
      spark.sql(
        """
          SELECT
            customer_id,
            name,
            city,
            department,
            purchase_amount,
            purchase_count,
            customer_segment
          FROM customers
          ORDER BY purchase_amount DESC
          LIMIT 5
        """
      )

    topCustomers.show(
      truncate = false
    )


    // ============================================================
    // 19. CUSTOMER ANALYTICS REPORT
    // ============================================================

    println()
    println("19. CUSTOMER ANALYTICS REPORT")
    println("============================================================")

    val analyticsReport =
      spark.sql(
        """
          SELECT
            customer_segment,
            COUNT(*) AS customers,
            ROUND(SUM(purchase_amount), 2) AS total_revenue,
            ROUND(AVG(purchase_amount), 2) AS average_revenue,
            SUM(purchase_count) AS total_purchases
          FROM customers
          GROUP BY customer_segment
          ORDER BY total_revenue DESC
        """
      )

    analyticsReport.show(
      truncate = false
    )


    // ============================================================
    // 20. READ JSON DATA
    // ============================================================

    println()
    println("20. READ JSON DATA")
    println("------------------------------------------------------------")

    val jsonPath = "data/customers.json"

    val customersJSON =
      spark.read
        .option("inferSchema", "true")
        .json(jsonPath)

    println("JSON DataFrame created successfully.")

    customersJSON.show(
      truncate = false
    )


    // ============================================================
    // 21. JSON SCHEMA
    // ============================================================

    println()
    println("21. JSON SCHEMA")
    println("------------------------------------------------------------")

    customersJSON.printSchema()


    // ============================================================
    // 22. DATAFRAME API VS SQL
    // ============================================================

    println()
    println("22. DATAFRAME API VS SQL")
    println("------------------------------------------------------------")

    println(
      """
DataFrame API:

customersCSV
  .filter(...)
  .select(...)
  .withColumn(...)

Spark SQL:

SELECT ...
FROM customers
WHERE ...

Both can be used to perform structured data processing.
"""
    )


    // ============================================================
    // 23. SCENARIO EXPLANATION
    // ============================================================

    println()
    println("23. CUSTOMER ANALYTICS SCENARIO")
    println("------------------------------------------------------------")

    println(
      """
Business Scenario:

A company has customer purchase data.

Required analytics:

1. Identify high-value customers.
2. Calculate average purchase value.
3. Segment customers.
4. Calculate revenue by city.
5. Calculate revenue by department.
6. Analyze customer segments.
7. Find top customers.

Spark SQL Solution:

CSV / JSON
    |
    v
DataFrame
    |
    +--> Schema inspection
    |
    +--> Select / Filter
    |
    +--> withColumn
    |
    v
Temporary View
    |
    v
Spark SQL
    |
    +--> Aggregation
    +--> Group By
    +--> Order By
    +--> Filtering
    |
    v
Customer Analytics Report
"""
    )


    // ============================================================
    // 24. IMPORTANT CONCEPTS
    // ============================================================

    println()
    println("24. IMPORTANT CONCEPTS")
    println("------------------------------------------------------------")

    println(
      """
DataFrame
    -> Distributed table-like data structure

Schema
    -> Structure and data types of DataFrame columns

select()
    -> Select required columns

filter()
    -> Keep rows satisfying a condition

withColumn()
    -> Add or replace a column

Temporary View
    -> Gives a DataFrame a SQL table-like name

spark.sql()
    -> Executes SQL against Spark tables/views

groupBy()
    -> Groups rows based on column values

ORDER BY
    -> Sorts SQL query results

COUNT()
    -> Counts rows

SUM()
    -> Calculates total

AVG()
    -> Calculates average
"""
    )


    // ============================================================
    // 25. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 13 COMPLETED")
    println("============================================================")

    println()
    println("Completed:")
    println("1. Created DataFrame from CSV")
    println("2. Created DataFrame from JSON")
    println("3. Inspected DataFrame schema")
    println("4. Selected columns")
    println("5. Filtered rows")
    println("6. Created columns using withColumn")
    println("7. Used Spark SQL expressions")
    println("8. Created temporary SQL view")
    println("9. Executed SQL queries")
    println("10. Built customer analytics report")

    println()
    println("============================================================")


    // ============================================================
    // STOP SPARK
    // ============================================================

    spark.stop()
  }
}
