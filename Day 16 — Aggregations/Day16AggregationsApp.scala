import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Day16AggregationsApp {

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
      .appName("Day 16 - Spark Aggregations")
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("============================================================")
    println("DAY 16 - SPARK AGGREGATIONS")
    println("============================================================")
    println(s"Spark Master       : $master")
    println(
      s"Default Parallelism: ${spark.sparkContext.defaultParallelism}"
    )
    println("============================================================")


    // ============================================================
    // 2. READ HOSPITAL TRANSACTION DATA
    // ============================================================

    println()
    println("2. READ HOSPITAL TRANSACTIONS")
    println("------------------------------------------------------------")

    val hospitalDF: DataFrame =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/hospital_transactions.csv")

    hospitalDF.show(
      numRows = 10,
      truncate = false
    )


    // ============================================================
    // 3. INSPECT SCHEMA
    // ============================================================

    println()
    println("3. HOSPITAL DATA SCHEMA")
    println("------------------------------------------------------------")

    hospitalDF.printSchema()


    // ============================================================
    // 4. BASIC COUNT
    // ============================================================

    println()
    println("4. COUNT")
    println("------------------------------------------------------------")

    val totalTransactions =
      hospitalDF.count()

    println(
      s"Total hospital transactions = $totalTransactions"
    )


    // ============================================================
    // 5. SUM
    // ============================================================

    println()
    println("5. SUM")
    println("------------------------------------------------------------")

    val totalRevenue =
      hospitalDF
        .agg(
          round(
            sum("service_amount"),
            2
          ).alias("total_revenue")
        )

    totalRevenue.show()


    // ============================================================
    // 6. AVG
    // ============================================================

    println()
    println("6. AVERAGE")
    println("------------------------------------------------------------")

    val averageRevenue =
      hospitalDF
        .agg(
          round(
            avg("service_amount"),
            2
          ).alias("average_service_amount")
        )

    averageRevenue.show()


    // ============================================================
    // 7. MIN
    // ============================================================

    println()
    println("7. MINIMUM")
    println("------------------------------------------------------------")

    val minimumRevenue =
      hospitalDF
        .agg(
          min("service_amount")
            .alias("minimum_service_amount")
        )

    minimumRevenue.show()


    // ============================================================
    // 8. MAX
    // ============================================================

    println()
    println("8. MAXIMUM")
    println("------------------------------------------------------------")

    val maximumRevenue =
      hospitalDF
        .agg(
          max("service_amount")
            .alias("maximum_service_amount")
        )

    maximumRevenue.show()


    // ============================================================
    // 9. ALL BASIC AGGREGATIONS TOGETHER
    // ============================================================

    println()
    println("9. ALL BASIC AGGREGATIONS")
    println("------------------------------------------------------------")

    val overallStatistics =
      hospitalDF
        .agg(
          count("*").alias("transaction_count"),

          round(
            sum("service_amount"),
            2
          ).alias("total_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_revenue"),

          min("service_amount")
            .alias("minimum_revenue"),

          max("service_amount")
            .alias("maximum_revenue")
        )

    overallStatistics.show(
      truncate = false
    )


    // ============================================================
    // 10. GROUP BY DEPARTMENT
    // ============================================================

    println()
    println("10. DEPARTMENT-WISE REVENUE")
    println("------------------------------------------------------------")

    val departmentRevenue =
      hospitalDF
        .groupBy("department")
        .agg(
          count("*")
            .alias("transaction_count"),

          round(
            sum("service_amount"),
            2
          ).alias("total_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_revenue"),

          min("service_amount")
            .alias("minimum_revenue"),

          max("service_amount")
            .alias("maximum_revenue")
        )
        .orderBy(
          col("total_revenue").desc
        )

    departmentRevenue.show(
      truncate = false
    )


    // ============================================================
    // 11. GROUP BY MULTIPLE COLUMNS
    // ============================================================

    println()
    println("11. GROUP BY DEPARTMENT + CITY")
    println("------------------------------------------------------------")

    val departmentCityRevenue =
      hospitalDF
        .groupBy(
          "department",
          "city"
        )
        .agg(
          count("*")
            .alias("transaction_count"),

          round(
            sum("service_amount"),
            2
          ).alias("total_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_revenue")
        )
        .orderBy(
          col("department"),
          col("total_revenue").desc
        )

    departmentCityRevenue.show(
      truncate = false
    )


    // ============================================================
    // 12. GROUP BY DEPARTMENT + SERVICE TYPE
    // ============================================================

    println()
    println("12. GROUP BY DEPARTMENT + SERVICE TYPE")
    println("------------------------------------------------------------")

    val departmentServiceRevenue =
      hospitalDF
        .groupBy(
          "department",
          "service_type"
        )
        .agg(
          count("*")
            .alias("transaction_count"),

          round(
            sum("service_amount"),
            2
          ).alias("total_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_revenue")
        )
        .orderBy(
          col("total_revenue").desc
        )

    departmentServiceRevenue.show(
      truncate = false
    )


    // ============================================================
    // 13. HAVING-LIKE FILTERING
    // ============================================================

    println()
    println("13. HAVING-LIKE FILTERING")
    println("------------------------------------------------------------")

    /*
     * SQL:
     *
     * SELECT department,
     *        SUM(service_amount) AS total_revenue
     * FROM hospital
     * GROUP BY department
     * HAVING SUM(service_amount) > 300000;
     *
     * DataFrame equivalent:
     *
     * groupBy()
     *   .agg()
     *   .filter()
     */

    val highRevenueDepartments =
      hospitalDF
        .groupBy("department")
        .agg(
          count("*")
            .alias("transaction_count"),

          round(
            sum("service_amount"),
            2
          ).alias("total_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_revenue")
        )
        .filter(
          col("total_revenue") > 300000
        )
        .orderBy(
          col("total_revenue").desc
        )

    highRevenueDepartments.show(
      truncate = false
    )


    // ============================================================
    // 14. HAVING-LIKE FILTER USING COUNT
    // ============================================================

    println()
    println("14. DEPARTMENTS WITH MORE THAN 5 TRANSACTIONS")
    println("------------------------------------------------------------")

    val frequentDepartments =
      hospitalDF
        .groupBy("department")
        .agg(
          count("*")
            .alias("transaction_count"),

          round(
            sum("service_amount"),
            2
          ).alias("total_revenue")
        )
        .filter(
          col("transaction_count") > 5
        )
        .orderBy(
          col("transaction_count").desc
        )

    frequentDepartments.show(
      truncate = false
    )


    // ============================================================
    // 15. DEPARTMENT-WISE DOCTOR STATISTICS
    // ============================================================

    println()
    println("15. DEPARTMENT + DOCTOR STATISTICS")
    println("------------------------------------------------------------")

    val doctorStatistics =
      hospitalDF
        .groupBy(
          "department",
          "doctor"
        )
        .agg(
          count("*")
            .alias("patient_count"),

          round(
            sum("service_amount"),
            2
          ).alias("total_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_revenue"),

          min("service_amount")
            .alias("minimum_revenue"),

          max("service_amount")
            .alias("maximum_revenue")
        )
        .orderBy(
          col("total_revenue").desc
        )

    doctorStatistics.show(
      truncate = false
    )


    // ============================================================
    // 16. HOSPITAL DEPARTMENT REVENUE METRICS
    // ============================================================

    println()
    println("16. HOSPITAL DEPARTMENT REVENUE METRICS")
    println("------------------------------------------------------------")

    val departmentMetrics =
      hospitalDF
        .groupBy("department")
        .agg(

          count("*")
            .alias("patient_visits"),

          round(
            sum("service_amount"),
            2
          ).alias("gross_revenue"),

          round(
            sum("insurance_amount"),
            2
          ).alias("insurance_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_bill"),

          min("service_amount")
            .alias("minimum_bill"),

          max("service_amount")
            .alias("maximum_bill"),

          round(
            avg("days_admitted"),
            2
          ).alias("average_days_admitted"),

          round(
            sum("doctor_fee"),
            2
          ).alias("total_doctor_fees")
        )
        .orderBy(
          col("gross_revenue").desc
        )

    departmentMetrics.show(
      truncate = false
    )


    // ============================================================
    // 17. CALCULATE HOSPITAL NET REVENUE
    // ============================================================

    println()
    println("17. NET REVENUE")
    println("------------------------------------------------------------")

    val departmentNetRevenue =
      departmentMetrics
        .withColumn(
          "net_revenue",
          round(
            col("gross_revenue") -
              col("insurance_revenue"),
            2
          )
        )
        .select(
          "department",
          "patient_visits",
          "gross_revenue",
          "insurance_revenue",
          "net_revenue",
          "average_bill",
          "total_doctor_fees"
        )
        .orderBy(
          col("net_revenue").desc
        )

    departmentNetRevenue.show(
      truncate = false
    )


    // ============================================================
    // 18. HIGH-VALUE DEPARTMENTS
    // ============================================================

    println()
    println("18. HIGH-VALUE DEPARTMENTS")
    println("------------------------------------------------------------")

    val highValueDepartments =
      departmentMetrics
        .filter(
          col("average_bill") > 30000
        )
        .select(
          "department",
          "patient_visits",
          "gross_revenue",
          "average_bill",
          "maximum_bill"
        )
        .orderBy(
          col("gross_revenue").desc
        )

    highValueDepartments.show(
      truncate = false
    )


    // ============================================================
    // 19. REGISTER TEMPORARY VIEW
    // ============================================================

    println()
    println("19. CREATE TEMPORARY VIEW")
    println("------------------------------------------------------------")

    hospitalDF
      .createOrReplaceTempView("hospital_transactions")

    println(
      "Temporary view 'hospital_transactions' created."
    )


    // ============================================================
    // 20. AGGREGATION USING SQL
    // ============================================================

    println()
    println("20. HOSPITAL REVENUE USING SQL")
    println("------------------------------------------------------------")

    val sqlDepartmentRevenue =
      spark.sql(
        """
          SELECT
            department,
            COUNT(*) AS patient_visits,
            ROUND(SUM(service_amount), 2)
              AS gross_revenue,
            ROUND(AVG(service_amount), 2)
              AS average_bill,
            MIN(service_amount)
              AS minimum_bill,
            MAX(service_amount)
              AS maximum_bill
          FROM hospital_transactions
          GROUP BY department
          ORDER BY gross_revenue DESC
        """
      )

    sqlDepartmentRevenue.show(
      truncate = false
    )


    // ============================================================
    // 21. SQL HAVING
    // ============================================================

    println()
    println("21. SQL HAVING")
    println("------------------------------------------------------------")

    val sqlHaving =
      spark.sql(
        """
          SELECT
            department,
            COUNT(*) AS patient_visits,
            ROUND(SUM(service_amount), 2)
              AS total_revenue,
            ROUND(AVG(service_amount), 2)
              AS average_bill
          FROM hospital_transactions
          GROUP BY department
          HAVING SUM(service_amount) > 300000
          ORDER BY total_revenue DESC
        """
      )

    sqlHaving.show(
      truncate = false
    )


    // ============================================================
    // 22. MULTIPLE COLUMN SQL GROUP BY
    // ============================================================

    println()
    println("22. SQL GROUP BY DEPARTMENT + CITY")
    println("------------------------------------------------------------")

    val sqlMultipleGroupBy =
      spark.sql(
        """
          SELECT
            department,
            city,
            COUNT(*) AS patient_visits,
            ROUND(SUM(service_amount), 2)
              AS total_revenue,
            ROUND(AVG(service_amount), 2)
              AS average_bill
          FROM hospital_transactions
          GROUP BY department, city
          ORDER BY department, total_revenue DESC
        """
      )

    sqlMultipleGroupBy.show(
      truncate = false
    )


    // ============================================================
    // 23. DEPARTMENT-WISE SALARY STATISTICS
    // ============================================================

    println()
    println("23. DEPARTMENT-WISE SALARY STATISTICS")
    println("------------------------------------------------------------")

    /*
     * This section uses doctor_fee as the available salary-like
     * numeric field in our hospital dataset.
     *
     * It demonstrates the exact aggregation pattern:
     *
     * department-wise:
     *     count
     *     sum
     *     average
     *     minimum
     *     maximum
     */

    val departmentSalaryStatistics =
      hospitalDF
        .groupBy("department")
        .agg(
          count("*")
            .alias("employee_records"),

          round(
            sum("doctor_fee"),
            2
          ).alias("total_salary_cost"),

          round(
            avg("doctor_fee"),
            2
          ).alias("average_salary"),

          min("doctor_fee")
            .alias("minimum_salary"),

          max("doctor_fee")
            .alias("maximum_salary")
        )
        .orderBy(
          col("total_salary_cost").desc
        )

    departmentSalaryStatistics.show(
      truncate = false
    )


    // ============================================================
    // 24. TOP REVENUE DEPARTMENT
    // ============================================================

    println()
    println("24. TOP REVENUE DEPARTMENT")
    println("------------------------------------------------------------")

    val topRevenueDepartment =
      departmentMetrics
        .orderBy(
          col("gross_revenue").desc
        )
        .limit(1)

    topRevenueDepartment.show(
      truncate = false
    )


    // ============================================================
    // 25. FINAL HOSPITAL ANALYTICS REPORT
    // ============================================================

    println()
    println("25. FINAL HOSPITAL ANALYTICS REPORT")
    println("============================================================")

    val finalReport =
      hospitalDF
        .groupBy("department")
        .agg(
          count("*")
            .alias("patient_visits"),

          round(
            sum("service_amount"),
            2
          ).alias("gross_revenue"),

          round(
            sum("insurance_amount"),
            2
          ).alias("insurance_claims"),

          round(
            sum("service_amount") -
              sum("insurance_amount"),
            2
          ).alias("net_revenue"),

          round(
            avg("service_amount"),
            2
          ).alias("average_bill"),

          min("service_amount")
            .alias("minimum_bill"),

          max("service_amount")
            .alias("maximum_bill"),

          round(
            avg("days_admitted"),
            2
          ).alias("average_admission_days")
        )
        .orderBy(
          col("gross_revenue").desc
        )

    finalReport.show(
      truncate = false
    )


    // ============================================================
    // 26. EXPLAIN AGGREGATION CONCEPTS
    // ============================================================

    println()
    println("26. AGGREGATION CONCEPTS")
    println("------------------------------------------------------------")

    println(
      """
COUNT
-----
Counts rows.

SUM
---
Adds numeric values.

AVG
---
Calculates average.

MIN
---
Finds smallest value.

MAX
---
Finds largest value.

GROUP BY
--------
Groups rows before aggregation.

HAVING
------
Filters groups after aggregation.

DataFrame equivalent:

groupBy()
    |
    v
agg()
    |
    v
filter()

SQL equivalent:

GROUP BY
    |
    v
HAVING
"""
    )


    // ============================================================
    // 27. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 16 COMPLETED")
    println("============================================================")

    println()
    println("Completed:")
    println("1. count()")
    println("2. sum()")
    println("3. avg()")
    println("4. min()")
    println("5. max()")
    println("6. groupBy()")
    println("7. groupBy() with multiple columns")
    println("8. HAVING-like filtering")
    println("9. Department-wise statistics")
    println("10. Hospital revenue metrics")
    println("11. Spark SQL aggregation")
    println("12. SQL HAVING")

    println()
    println(
      "Key concept: aggregation summarizes multiple rows into " +
      "business-level metrics."
    )

    println()
    println("============================================================")


    // ============================================================
    // STOP SPARK
    // ============================================================

    spark.stop()
  }
}
