import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Day14DatasetApp {

  // ============================================================
  // CASE CLASS - EMPLOYEE
  // ============================================================

  case class Employee(
      employee_id: String,
      name: String,
      department: String,
      designation: String,
      base_salary: Double,
      bonus: Double,
      working_days: Int
  )

  // ============================================================
  // CASE CLASS - PAYROLL
  // ============================================================

  case class EmployeePayroll(
      employee_id: String,
      name: String,
      department: String,
      designation: String,
      base_salary: Double,
      bonus: Double,
      working_days: Int,
      gross_salary: Double,
      tax: Double,
      net_salary: Double
  )

  // ============================================================
  // MAIN
  // ============================================================

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
      .appName("Day 14 - DataFrame and Dataset")
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println()
    println("============================================================")
    println("DAY 14 - DATAFRAME AND DATASET")
    println("============================================================")
    println(s"Spark Master       : $master")
    println(
      s"Default Parallelism: ${spark.sparkContext.defaultParallelism}"
    )
    println("============================================================")


    // ============================================================
    // 2. READ CSV AS DATAFRAME
    // ============================================================

    println()
    println("2. READ EMPLOYEE CSV AS DATAFRAME")
    println("------------------------------------------------------------")

    val employeeDF: DataFrame =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/employees.csv")

    employeeDF.show(
      truncate = false
    )


    // ============================================================
    // 3. INSPECT DATAFRAME SCHEMA
    // ============================================================

    println()
    println("3. DATAFRAME SCHEMA")
    println("------------------------------------------------------------")

    employeeDF.printSchema()


    // ============================================================
    // 4. DATAFRAME OPERATIONS
    // ============================================================

    println()
    println("4. DATAFRAME OPERATIONS")
    println("------------------------------------------------------------")

    println("Engineering employees:")

    val engineeringDF =
      employeeDF
        .filter(
          col("department") === "Engineering"
        )
        .select(
          "employee_id",
          "name",
          "designation",
          "base_salary"
        )

    engineeringDF.show(
      truncate = false
    )


    // ============================================================
    // 5. CONVERT DATAFRAME TO DATASET
    // ============================================================

    println()
    println("5. DATAFRAME -> DATASET")
    println("------------------------------------------------------------")

    /*
     * to[Employee] converts the generic DataFrame
     * into a strongly typed Dataset[Employee].
     *
     * The case class Employee defines the expected structure.
     */

    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

    println(
      "DataFrame successfully converted to Dataset[Employee]."
    )

    employeeDS.show(
      truncate = false
    )


    // ============================================================
    // 6. DATASET TYPE INFORMATION
    // ============================================================

    println()
    println("6. DATASET TYPE INFORMATION")
    println("------------------------------------------------------------")

    println(
      s"Dataset type = ${employeeDS.getClass.getName}"
    )

    println(
      "Logical type = Dataset[Employee]"
    )


    // ============================================================
    // 7. TYPED DATASET FILTER
    // ============================================================

    println()
    println("7. TYPED DATASET FILTER")
    println("------------------------------------------------------------")

    val highSalaryEmployees =
      employeeDS.filter(
        employee => employee.base_salary > 80000
      )

    highSalaryEmployees.show(
      truncate = false
    )


    // ============================================================
    // 8. TYPED DATASET MAP
    // ============================================================

    println()
    println("8. TYPED DATASET MAP")
    println("------------------------------------------------------------")

    /*
     * Calculate gross salary:
     *
     * gross_salary =
     * base_salary + bonus
     */

    val grossSalaryDS: Dataset[(String, String, Double)] =
      employeeDS.map { employee =>

        val grossSalary =
          employee.base_salary + employee.bonus

        (
          employee.employee_id,
          employee.name,
          grossSalary
        )
      }

    grossSalaryDS.show(
      truncate = false
    )


    // ============================================================
    // 9. CREATE TYPED PAYROLL DATASET
    // ============================================================

    println()
    println("9. CREATE TYPED PAYROLL DATASET")
    println("------------------------------------------------------------")

    /*
     * Payroll calculation:
     *
     * gross_salary =
     * base_salary + bonus
     *
     * tax =
     * gross_salary * 10%
     *
     * net_salary =
     * gross_salary - tax
     */

    val payrollDS: Dataset[EmployeePayroll] =
      employeeDS.map { employee =>

        val grossSalary =
          employee.base_salary + employee.bonus

        val tax =
          grossSalary * 0.10

        val netSalary =
          grossSalary - tax

        EmployeePayroll(
          employee_id = employee.employee_id,
          name = employee.name,
          department = employee.department,
          designation = employee.designation,
          base_salary = employee.base_salary,
          bonus = employee.bonus,
          working_days = employee.working_days,
          gross_salary = grossSalary,
          tax = tax,
          net_salary = netSalary
        )
      }

    payrollDS.show(
      truncate = false
    )


    // ============================================================
    // 10. PAYROLL SCHEMA
    // ============================================================

    println()
    println("10. PAYROLL DATASET SCHEMA")
    println("------------------------------------------------------------")

    payrollDS.printSchema()


    // ============================================================
    // 11. FILTER TYPED PAYROLL DATASET
    // ============================================================

    println()
    println("11. HIGH NET-SALARY EMPLOYEES")
    println("------------------------------------------------------------")

    val highNetSalaryEmployees =
      payrollDS.filter(
        employee => employee.net_salary > 90000
      )

    highNetSalaryEmployees.show(
      truncate = false
    )


    // ============================================================
    // 12. CONVERT DATASET BACK TO DATAFRAME
    // ============================================================

    println()
    println("12. DATASET -> DATAFRAME")
    println("------------------------------------------------------------")

    val payrollDF: DataFrame =
      payrollDS.toDF()

    payrollDF.show(
      truncate = false
    )

    payrollDF.printSchema()


    // ============================================================
    // 13. DATAFRAME OPERATIONS ON PAYROLL
    // ============================================================

    println()
    println("13. PAYROLL DATAFRAME OPERATIONS")
    println("------------------------------------------------------------")

    val selectedPayroll =
      payrollDF.select(
        "employee_id",
        "name",
        "department",
        "gross_salary",
        "tax",
        "net_salary"
      )

    selectedPayroll.show(
      truncate = false
    )


    // ============================================================
    // 14. REORDER PAYROLL BY NET SALARY
    // ============================================================

    println()
    println("14. PAYROLL SORTED BY NET SALARY")
    println("------------------------------------------------------------")

    val sortedPayroll =
      payrollDF
        .orderBy(
          col("net_salary").desc
        )

    sortedPayroll.show(
      truncate = false
    )


    // ============================================================
    // 15. DEPARTMENT PAYROLL ANALYSIS
    // ============================================================

    println()
    println("15. DEPARTMENT PAYROLL ANALYSIS")
    println("------------------------------------------------------------")

    val departmentPayroll =
      payrollDF
        .groupBy("department")
        .agg(
          count("*").alias("employee_count"),
          round(sum("gross_salary"), 2)
            .alias("total_gross_salary"),
          round(avg("gross_salary"), 2)
            .alias("average_gross_salary"),
          round(sum("net_salary"), 2)
            .alias("total_net_salary")
        )
        .orderBy(
          col("total_gross_salary").desc
        )

    departmentPayroll.show(
      truncate = false
    )


    // ============================================================
    // 16. DATASET FILTER + MAP SCENARIO
    // ============================================================

    println()
    println("16. TYPED PAYROLL ANALYSIS")
    println("------------------------------------------------------------")

    val engineeringPayroll =
      payrollDS
        .filter(
          employee =>
            employee.department == "Engineering"
        )
        .map { employee =>

          (
            employee.employee_id,
            employee.name,
            employee.net_salary
          )
        }

    engineeringPayroll.show(
      truncate = false
    )


    // ============================================================
    // 17. DATASET -> DATAFRAME -> SQL
    // ============================================================

    println()
    println("17. DATASET -> DATAFRAME -> SQL")
    println("------------------------------------------------------------")

    payrollDF.createOrReplaceTempView("employee_payroll")

    val sqlPayroll =
      spark.sql(
        """
          SELECT
            employee_id,
            name,
            department,
            designation,
            gross_salary,
            tax,
            net_salary
          FROM employee_payroll
          WHERE net_salary > 80000
          ORDER BY net_salary DESC
        """
      )

    sqlPayroll.show(
      truncate = false
    )


    // ============================================================
    // 18. TOTAL PAYROLL
    // ============================================================

    println()
    println("18. TOTAL PAYROLL")
    println("------------------------------------------------------------")

    val totalPayroll =
      payrollDF
        .agg(
          round(
            sum("gross_salary"),
            2
          ).alias("total_gross_payroll"),

          round(
            sum("tax"),
            2
          ).alias("total_tax"),

          round(
            sum("net_salary"),
            2
          ).alias("total_net_payroll")
        )

    totalPayroll.show(
      truncate = false
    )


    // ============================================================
    // 19. EMPLOYEE PAYROLL SCENARIO
    // ============================================================

    println()
    println("19. EMPLOYEE PAYROLL SCENARIO")
    println("------------------------------------------------------------")

    println(
      """
Business Scenario:

A company has employee information.

Input:
    Employee CSV

Step 1:
    Read CSV as DataFrame

Step 2:
    Convert DataFrame to Dataset[Employee]

Step 3:
    Perform typed transformations

Step 4:
    Calculate:

        Gross Salary
        Tax
        Net Salary

Step 5:
    Store result as Dataset[EmployeePayroll]

Step 6:
    Convert Dataset to DataFrame

Step 7:
    Perform analytical queries

Step 8:
    Use Spark SQL for reporting

Pipeline:

employees.csv
     |
     v
DataFrame
     |
     | .as[Employee]
     v
Dataset[Employee]
     |
     | typed map/filter
     v
Dataset[EmployeePayroll]
     |
     | .toDF()
     v
DataFrame
     |
     +--------+---------+
     |        |         |
   SQL     groupBy    orderBy
     |        |         |
     +--------+---------+
              |
              v
       Payroll Report
"""
    )


    // ============================================================
    // 20. RDD vs DATAFRAME vs DATASET
    // ============================================================

    println()
    println("20. RDD vs DATAFRAME vs DATASET")
    println("------------------------------------------------------------")

    println(
      """
RDD
---
Low-level distributed collection.

Example:
    RDD[Employee]

Advantages:
    - Fine-grained control
    - Flexible object processing

Disadvantages:
    - Less structured
    - Less optimization compared with DataFrame/Dataset APIs


DataFrame
---------
Distributed data organized into named columns.

Conceptually:
    Dataset[Row]

Advantages:
    - Schema
    - SQL support
    - Catalyst optimization
    - Easy analytical operations

Disadvantages:
    - Columns are accessed more generically
    - Less compile-time type safety than Dataset


Dataset
-------
Strongly typed distributed collection.

Example:
    Dataset[Employee]

Advantages:
    - Compile-time type safety
    - Case classes
    - Functional transformations
    - Catalyst optimization
    - Encoder support

Disadvantages:
    - Scala-specific typed API
    - Requires appropriate encoders
"""
    )


    // ============================================================
    // 21. TYPE SAFETY
    // ============================================================

    println()
    println("21. TYPE SAFETY")
    println("------------------------------------------------------------")

    println(
      """
DataFrame:

DataFrame
    |
    v
Row
    |
    v
Column access

Example:
    row.getAs[Double]("base_salary")


Dataset:

Dataset[Employee]
    |
    v
Employee
    |
    v
employee.base_salary

The compiler knows the expected Employee type.

This provides stronger compile-time type checking.
"""
    )


    // ============================================================
    // 22. CATALYST OPTIMIZATION
    // ============================================================

    println()
    println("22. CATALYST OPTIMIZATION")
    println("------------------------------------------------------------")

    println(
      """
Catalyst is Spark SQL's query optimization framework.

Typical flow:

DataFrame / Dataset operation
            |
            v
      Logical Plan
            |
            v
   Catalyst Optimization
            |
            v
      Physical Plan
            |
            v
       Execution


Example:

payrollDF
  .filter(net_salary > 90000)
  .select(name, net_salary)

Spark can analyze the query plan and optimize execution.
"""
    )


    // ============================================================
    // 23. EXPLAIN QUERY PLAN
    // ============================================================

    println()
    println("23. EXPLAIN QUERY PLAN")
    println("------------------------------------------------------------")

    println(
      "Physical execution plan:"
    )

    payrollDF
      .filter(
        col("net_salary") > 90000
      )
      .select(
        "name",
        "department",
        "net_salary"
      )
      .explain()


    // ============================================================
    // 24. KEY DIFFERENCES
    // ============================================================

    println()
    println("24. KEY DIFFERENCES")
    println("------------------------------------------------------------")

    println(
      """
RDD:
    Low-level
    No automatic schema
    Compile-time type safety for Scala objects
    Less SQL optimization

DataFrame:
    Structured
    Schema
    Dataset[Row]
    Catalyst optimized
    Less compile-time field/type checking

Dataset:
    Structured
    Schema
    Strongly typed
    Catalyst optimized
    Case-class based
"""
    )


    // ============================================================
    // 25. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 14 COMPLETED")
    println("============================================================")

    println()
    println("Completed:")
    println("1. Created Employee case class")
    println("2. Created EmployeePayroll case class")
    println("3. Read CSV as DataFrame")
    println("4. Converted DataFrame -> Dataset[Employee]")
    println("5. Used typed Dataset filter")
    println("6. Used typed Dataset map")
    println("7. Built typed payroll pipeline")
    println("8. Converted Dataset -> DataFrame")
    println("9. Performed DataFrame analytics")
    println("10. Used Spark SQL")
    println("11. Compared RDD/DataFrame/Dataset")
    println("12. Explained type safety")
    println("13. Explained Catalyst optimization")

    println()
    println("============================================================")


    // ============================================================
    // STOP SPARK
    // ============================================================

    spark.stop()
  }
}
