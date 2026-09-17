import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Day15UDFApp {

  // ============================================================
  // 1. NORMAL SCALA FUNCTION - SALARY BAND
  // ============================================================

  def salaryBand(salary: Double): String = {

    if (salary < 40000) {
      "Low"
    } else if (salary < 70000) {
      "Medium"
    } else if (salary < 100000) {
      "High"
    } else {
      "Premium"
    }
  }


  // ============================================================
  // 2. NORMAL SCALA FUNCTION - RISK CATEGORY
  // ============================================================

  def riskCategory(amount: Double): String = {

    if (amount < 25000) {
      "Low Risk"
    } else if (amount < 75000) {
      "Medium Risk"
    } else if (amount < 150000) {
      "High Risk"
    } else {
      "Critical Risk"
    }
  }


  // ============================================================
  // 3. NORMAL SCALA FUNCTION - TRANSACTION FREQUENCY
  // ============================================================

  def transactionFrequency(count: Int): String = {

    if (count <= 3) {
      "Low Frequency"
    } else if (count <= 8) {
      "Medium Frequency"
    } else {
      "High Frequency"
    }
  }


  // ============================================================
  // MAIN
  // ============================================================

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 4. CREATE SPARK SESSION
    // ============================================================

    val cores =
      if (args.nonEmpty) args(0)
      else "4"

    val master = s"local[$cores]"

    val spark = SparkSession
      .builder()
      .appName("Day 15 - UDF Practice")
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("============================================================")
    println("DAY 15 - UDF PRACTICE")
    println("============================================================")
    println(s"Spark Master       : $master")
    println(
      s"Default Parallelism: ${spark.sparkContext.defaultParallelism}"
    )
    println("============================================================")


    // ============================================================
    // 5. READ CUSTOMER TRANSACTION DATA
    // ============================================================

    println()
    println("5. READ CUSTOMER TRANSACTION DATA")
    println("------------------------------------------------------------")

    val transactionsDF: DataFrame =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/customer_transactions.csv")

    transactionsDF.show(
      truncate = false
    )


    // ============================================================
    // 6. INSPECT SCHEMA
    // ============================================================

    println()
    println("6. TRANSACTION SCHEMA")
    println("------------------------------------------------------------")

    transactionsDF.printSchema()


    // ============================================================
    // 7. CREATE SALARY DATAFRAME
    // ============================================================

    println()
    println("7. SALARY BAND UDF PRACTICE")
    println("------------------------------------------------------------")

    /*
     * This is a small example specifically for salary-band
     * classification.
     *
     * The transaction amount is used as a salary-like value
     * so that the same UDF concept can be demonstrated using
     * the current customer dataset.
     */

    val salaryExampleDF =
      transactionsDF.select(
        col("customer_id"),
        col("name"),
        col("transaction_amount").alias("salary")
      )

    salaryExampleDF.show(
      truncate = false
    )


    // ============================================================
    // 8. CREATE SALARY BAND UDF
    // ============================================================

    println()
    println("8. CREATE SCALA SALARY BAND UDF")
    println("------------------------------------------------------------")

    val salaryBandUDF =
      udf(salaryBand _)

    println(
      "Scala salaryBand function converted into a Spark UDF."
    )


    // ============================================================
    // 9. USE SALARY BAND UDF WITH WITHCOLUMN
    // ============================================================

    println()
    println("9. APPLY SALARY BAND UDF")
    println("------------------------------------------------------------")

    val salaryBandDF =
      salaryExampleDF.withColumn(
        "salary_band",
        salaryBandUDF(col("salary"))
      )

    salaryBandDF.show(
      truncate = false
    )


    // ============================================================
    // 10. CALCULATED COLUMN USING BUILT-IN FUNCTION
    // ============================================================

    println()
    println("10. BUILT-IN SPARK FUNCTION")
    println("------------------------------------------------------------")

    /*
     * Built-in Spark function:
     *
     * round()
     *
     * We calculate the average transaction value.
     */

    val calculatedDF =
      transactionsDF.withColumn(
        "average_transaction_value",
        round(
          col("transaction_amount") /
            col("transaction_count"),
          2
        )
      )

    calculatedDF.show(
      truncate = false
    )


    // ============================================================
    // 11. CREATE RISK CATEGORY UDF
    // ============================================================

    println()
    println("11. CREATE CUSTOMER RISK UDF")
    println("------------------------------------------------------------")

    val riskCategoryUDF =
      udf(riskCategory _)

    println(
      "riskCategory Scala function converted into a Spark UDF."
    )


    // ============================================================
    // 12. ADD RISK CATEGORY
    // ============================================================

    println()
    println("12. ADD RISK CATEGORY WITH WITHCOLUMN")
    println("------------------------------------------------------------")

    val riskDF =
      calculatedDF.withColumn(
        "risk_category",
        riskCategoryUDF(
          col("transaction_amount")
        )
      )

    riskDF.show(
      truncate = false
    )


    // ============================================================
    // 13. CREATE TRANSACTION FREQUENCY UDF
    // ============================================================

    println()
    println("13. TRANSACTION FREQUENCY UDF")
    println("------------------------------------------------------------")

    val transactionFrequencyUDF =
      udf(transactionFrequency _)

    val frequencyDF =
      riskDF.withColumn(
        "transaction_frequency",
        transactionFrequencyUDF(
          col("transaction_count")
        )
      )

    frequencyDF.show(
      truncate = false
    )


    // ============================================================
    // 14. COMBINED CUSTOMER RISK SCORE
    // ============================================================

    println()
    println("14. COMBINED CUSTOMER RISK ANALYSIS")
    println("------------------------------------------------------------")

    /*
     * Risk is determined from:
     *
     * transaction amount
     * transaction frequency
     *
     * This is an example of combining multiple calculated
     * columns with business logic.
     */

    val customerRiskDF =
      frequencyDF.withColumn(
        "risk_score",
        when(
          col("risk_category") === "Critical Risk",
          4
        )
          .when(
            col("risk_category") === "High Risk",
            3
          )
          .when(
            col("risk_category") === "Medium Risk",
            2
          )
          .otherwise(1)
      )

    customerRiskDF.show(
      truncate = false
    )


    // ============================================================
    // 15. FINAL RISK CATEGORY
    // ============================================================

    println()
    println("15. FINAL CUSTOMER RISK CATEGORY")
    println("------------------------------------------------------------")

    val finalRiskDF =
      customerRiskDF.withColumn(
        "final_risk_category",
        when(
          col("risk_score") === 4 &&
          col("transaction_frequency") === "High Frequency",
          "Critical"
        )
          .when(
            col("risk_score") >= 3,
            "High"
          )
          .when(
            col("risk_score") === 2,
            "Medium"
          )
          .otherwise("Low")
      )

    finalRiskDF
      .select(
        "customer_id",
        "name",
        "transaction_amount",
        "transaction_count",
        "risk_category",
        "transaction_frequency",
        "risk_score",
        "final_risk_category"
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 16. COMPARE UDF WITH BUILT-IN FUNCTION
    // ============================================================

    println()
    println("16. UDF VS BUILT-IN FUNCTION")
    println("------------------------------------------------------------")

    /*
     * Built-in Spark function:
     *
     * upper()
     *
     * UDF:
     *
     * salaryBandUDF()
     */

    val comparisonDF =
      finalRiskDF
        .withColumn(
          "name_upper_builtin",
          upper(col("name"))
        )
        .withColumn(
          "salary_band_udf",
          salaryBandUDF(
            col("transaction_amount")
          )
        )

    comparisonDF
      .select(
        "name",
        "name_upper_builtin",
        "transaction_amount",
        "salary_band_udf"
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 17. REGISTER UDF WITH SPARK SESSION / CATALOG
    // ============================================================

    println()
    println("17. REGISTER UDF WITH SPARK SQL CATALOG")
    println("------------------------------------------------------------")

    /*
     * Register the Scala function as a named SQL UDF.
     *
     * After registration we can call:
     *
     * salary_band(transaction_amount)
     *
     * directly from Spark SQL.
     */

    spark.udf.register(
      "salary_band",
      salaryBand _
    )

    println(
      "UDF 'salary_band' registered successfully."
    )


    // ============================================================
    // 18. CREATE TEMPORARY VIEW
    // ============================================================

    println()
    println("18. CREATE TEMPORARY VIEW")
    println("------------------------------------------------------------")

    finalRiskDF
      .createOrReplaceTempView("customer_transactions")

    println(
      "Temporary view 'customer_transactions' created."
    )


    // ============================================================
    // 19. USE REGISTERED UDF IN SQL
    // ============================================================

    println()
    println("19. USE REGISTERED UDF IN SQL")
    println("------------------------------------------------------------")

    val sqlSalaryBand =
      spark.sql(
        """
          SELECT
            customer_id,
            name,
            transaction_amount,
            salary_band(transaction_amount) AS salary_band
          FROM customer_transactions
          ORDER BY transaction_amount DESC
        """
      )

    sqlSalaryBand.show(
      truncate = false
    )


    // ============================================================
    // 20. REGISTER CUSTOMER RISK UDF
    // ============================================================

    println()
    println("20. REGISTER RISK UDF")
    println("------------------------------------------------------------")

    spark.udf.register(
      "customer_risk",
      riskCategory _
    )

    println(
      "UDF 'customer_risk' registered successfully."
    )


    // ============================================================
    // 21. USE RISK UDF FROM SQL
    // ============================================================

    println()
    println("21. CUSTOMER RISK USING SQL UDF")
    println("------------------------------------------------------------")

    val sqlRisk =
      spark.sql(
        """
          SELECT
            customer_id,
            name,
            city,
            transaction_amount,
            customer_risk(transaction_amount)
              AS risk_category
          FROM customer_transactions
          ORDER BY transaction_amount DESC
        """
      )

    sqlRisk.show(
      truncate = false
    )


    // ============================================================
    // 22. RISK CATEGORY SUMMARY
    // ============================================================

    println()
    println("22. RISK CATEGORY SUMMARY")
    println("------------------------------------------------------------")

    val riskSummary =
      finalRiskDF
        .groupBy("final_risk_category")
        .agg(
          count("*").alias("customer_count"),
          round(
            sum("transaction_amount"),
            2
          ).alias("total_transaction_value"),
          round(
            avg("transaction_amount"),
            2
          ).alias("average_transaction_value")
        )
        .orderBy(
          col("total_transaction_value").desc
        )

    riskSummary.show(
      truncate = false
    )


    // ============================================================
    // 23. HIGH-RISK CUSTOMERS
    // ============================================================

    println()
    println("23. HIGH-RISK CUSTOMERS")
    println("------------------------------------------------------------")

    val highRiskCustomers =
      finalRiskDF
        .filter(
          col("final_risk_category") === "High" ||
          col("final_risk_category") === "Critical"
        )
        .select(
          "customer_id",
          "name",
          "city",
          "transaction_amount",
          "transaction_count",
          "final_risk_category"
        )
        .orderBy(
          col("transaction_amount").desc
        )

    highRiskCustomers.show(
      truncate = false
    )


    // ============================================================
    // 24. CUSTOMER RISK ANALYTICS REPORT
    // ============================================================

    println()
    println("24. CUSTOMER RISK ANALYTICS REPORT")
    println("============================================================")

    val customerRiskReport =
      spark.sql(
        """
          SELECT
            final_risk_category,
            COUNT(*) AS customers,
            ROUND(SUM(transaction_amount), 2)
              AS total_transaction_value,
            ROUND(AVG(transaction_amount), 2)
              AS average_transaction_value,
            ROUND(MAX(transaction_amount), 2)
              AS maximum_transaction_value
          FROM customer_transactions
          GROUP BY final_risk_category
          ORDER BY total_transaction_value DESC
        """
      )

    customerRiskReport.show(
      truncate = false
    )


    // ============================================================
    // 25. EXPLAIN BUILT-IN FUNCTION VS UDF
    // ============================================================

    println()
    println("25. BUILT-IN FUNCTION VS UDF")
    println("------------------------------------------------------------")

    println(
      """
Built-in Spark function:

    upper()
    lower()
    round()
    abs()
    sqrt()
    when()

Advantages:
    - Implemented by Spark
    - Optimized by Spark
    - Usually preferred when available
    - Better integration with Spark SQL execution


UDF:

    User Defined Function

Example:

    salary_band(transaction_amount)

Advantages:
    - Allows custom business logic
    - Useful when no suitable built-in function exists

Disadvantages:
    - Can introduce serialization/execution overhead
    - Spark has less visibility into arbitrary UDF logic
    - Built-in functions are generally preferred when they
      can express the same requirement
"""
    )


    // ============================================================
    // 26. WHEN TO USE UDF
    // ============================================================

    println()
    println("26. WHEN TO USE A UDF")
    println("------------------------------------------------------------")

    println(
      """
Use a UDF when:

1. Business logic is custom.
2. No suitable built-in Spark function exists.
3. The transformation cannot be conveniently expressed using
   Spark SQL/DataFrame functions.

Example:

    transaction_amount
            |
            v
      custom business rule
            |
            v
       Risk Category


Avoid a UDF when an equivalent built-in Spark function exists.
"""
    )


    // ============================================================
    // 27. CUSTOMER RISK SCENARIO
    // ============================================================

    println()
    println("27. CUSTOMER RISK SCENARIO")
    println("------------------------------------------------------------")

    println(
      """
Business Scenario:

A company wants to classify customer transactions.

Input:

    customer_id
    transaction_amount
    transaction_count

Step 1:
    Read CSV

Step 2:
    Create DataFrame

Step 3:
    Create Scala riskCategory function

Step 4:
    Convert it into Spark UDF

Step 5:
    Add risk_category using withColumn

Step 6:
    Add transaction frequency

Step 7:
    Calculate risk score

Step 8:
    Create final risk category

Step 9:
    Register UDF with Spark SQL

Step 10:
    Use the UDF inside SQL

Pipeline:

Customer Transactions
        |
        v
    DataFrame
        |
        v
     UDF
        |
        +----------------+
        |                |
        v                v
 Risk Category     Frequency
        |                |
        +-------+--------+
                |
                v
           Risk Score
                |
                v
        Final Risk Category
                |
                v
        Analytics Report
"""
    )


    // ============================================================
    // 28. IMPORTANT UDF CONCEPTS
    // ============================================================

    println()
    println("28. IMPORTANT UDF CONCEPTS")
    println("------------------------------------------------------------")

    println(
      """
UDF
---
User Defined Function.

Scala Function
---
Normal Scala function written by the developer.

Spark UDF
---
A Spark-compatible wrapper around custom function logic.

withColumn()
---
Creates or replaces a DataFrame column.

spark.udf.register()
---
Registers a named UDF that can be used from Spark SQL.

Built-in Function
---
Function already provided by Spark.

Catalyst
---
Spark SQL optimizer that can optimize structured query plans.

Important rule:
---
Prefer built-in Spark functions when they can express the
required transformation. Use UDFs when custom logic is needed.
"""
    )


    // ============================================================
    // 29. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 15 COMPLETED")
    println("============================================================")

    println()
    println("Completed:")
    println("1. Created Scala salary-band function")
    println("2. Converted Scala function to Spark UDF")
    println("3. Used UDF with withColumn")
    println("4. Created customer risk UDF")
    println("5. Created transaction frequency UDF")
    println("6. Added calculated columns")
    println("7. Compared UDF with built-in Spark functions")
    println("8. Registered UDF with Spark SQL catalog")
    println("9. Used registered UDF from SQL")
    println("10. Built customer risk analytics report")

    println()
    println("Key concept:")
    println(
      "A UDF allows custom business logic to be applied to " +
      "Spark DataFrame or SQL data."
    )

    println()
    println("============================================================")


    // ============================================================
    // STOP SPARK
    // ============================================================

    spark.stop()
  }
}
