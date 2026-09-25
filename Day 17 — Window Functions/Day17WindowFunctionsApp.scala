import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17WindowFunctionsApp {

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
      .appName("Day 17 - Window Functions")
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("============================================================")
    println("DAY 17 - WINDOW FUNCTIONS")
    println("============================================================")
    println(s"Spark Master       : $master")
    println(
      s"Default Parallelism: ${spark.sparkContext.defaultParallelism}"
    )
    println("============================================================")


    // ============================================================
    // 2. READ STUDENT DATA
    // ============================================================

    println()
    println("2. READ STUDENT DATA")
    println("------------------------------------------------------------")

    val studentsDF: DataFrame =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/students.csv")

    studentsDF.show(
      truncate = false
    )


    // ============================================================
    // 3. STUDENT SCHEMA
    // ============================================================

    println()
    println("3. STUDENT SCHEMA")
    println("------------------------------------------------------------")

    studentsDF.printSchema()


    // ============================================================
    // 4. BASIC WINDOW SPECIFICATION
    // ============================================================

    println()
    println("4. WINDOW SPECIFICATION")
    println("------------------------------------------------------------")

    /*
     * Partition by course.
     *
     * This means:
     *
     * Data_Engineering students are considered together.
     * Spark students are considered together.
     * Kafka students are considered together.
     * Scala students are considered together.
     *
     * Within every course:
     *
     * order by marks descending.
     */

    val courseWindow =
      Window
        .partitionBy("course")
        .orderBy(
          col("marks").desc,
          col("student_id")
        )

    println(
      "Window created: partitionBy(course), orderBy(marks DESC)"
    )


    // ============================================================
    // 5. ROW_NUMBER
    // ============================================================

    println()
    println("5. ROW_NUMBER")
    println("------------------------------------------------------------")

    val studentsWithRowNumber =
      studentsDF.withColumn(
        "row_number",
        row_number().over(courseWindow)
      )

    studentsWithRowNumber
      .orderBy(
        "course",
        col("row_number")
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 6. RANK
    // ============================================================

    println()
    println("6. RANK")
    println("------------------------------------------------------------")

    val studentsWithRank =
      studentsDF.withColumn(
        "rank",
        rank().over(courseWindow)
      )

    studentsWithRank
      .orderBy(
        "course",
        col("rank")
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 7. DENSE_RANK
    // ============================================================

    println()
    println("7. DENSE_RANK")
    println("------------------------------------------------------------")

    val studentsWithDenseRank =
      studentsDF.withColumn(
        "dense_rank",
        dense_rank().over(courseWindow)
      )

    studentsWithDenseRank
      .orderBy(
        "course",
        col("dense_rank")
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 8. TOP 3 STUDENTS PER COURSE
    // ============================================================

    println()
    println("8. TOP 3 STUDENTS PER COURSE")
    println("------------------------------------------------------------")

    val rankedStudents =
      studentsDF.withColumn(
        "rank",
        dense_rank().over(
          Window
            .partitionBy("course")
            .orderBy(
              col("marks").desc
            )
        )
      )

    val top3Students =
      rankedStudents
        .filter(
          col("rank") <= 3
        )
        .select(
          "course",
          "student_id",
          "student_name",
          "marks",
          "rank"
        )
        .orderBy(
          "course",
          "rank",
          col("marks").desc
        )

    top3Students.show(
      truncate = false
    )


    // ============================================================
    // 9. TOP 3 EXACT ROWS PER COURSE
    // ============================================================

    println()
    println("9. TOP 3 EXACT ROWS PER COURSE")
    println("------------------------------------------------------------")

    val top3ExactStudents =
      studentsWithRowNumber
        .filter(
          col("row_number") <= 3
        )
        .select(
          "course",
          "student_id",
          "student_name",
          "marks",
          "row_number"
        )
        .orderBy(
          "course",
          "row_number"
        )

    top3ExactStudents.show(
      truncate = false
    )


    // ============================================================
    // 10. LAG
    // ============================================================

    println()
    println("10. LAG")
    println("------------------------------------------------------------")

    /*
     * lag() accesses a previous row inside the window.
     *
     * Here we look at the previous student's marks
     * within each course.
     */

    val studentsWithPreviousMarks =
      studentsDF.withColumn(
        "previous_marks",
        lag("marks", 1).over(courseWindow)
      )

    studentsWithPreviousMarks
      .orderBy(
        "course",
        col("marks").desc
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 11. MARK DIFFERENCE
    // ============================================================

    println()
    println("11. MARK DIFFERENCE USING LAG")
    println("------------------------------------------------------------")

    val studentsWithMarkDifference =
      studentsWithPreviousMarks.withColumn(
        "difference_from_previous",
        col("marks") -
          col("previous_marks")
      )

    studentsWithMarkDifference
      .orderBy(
        "course",
        col("marks").desc
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 12. LEAD
    // ============================================================

    println()
    println("12. LEAD")
    println("------------------------------------------------------------")

    /*
     * lead() accesses a future row.
     */

    val studentsWithNextMarks =
      studentsDF.withColumn(
        "next_marks",
        lead("marks", 1).over(courseWindow)
      )

    studentsWithNextMarks
      .orderBy(
        "course",
        col("marks").desc
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 13. STUDENT PERFORMANCE MOVEMENT
    // ============================================================

    println()
    println("13. STUDENT PERFORMANCE MOVEMENT")
    println("------------------------------------------------------------")

    val studentPerformance =
      studentsDF
        .withColumn(
          "previous_marks",
          lag("marks", 1).over(courseWindow)
        )
        .withColumn(
          "next_marks",
          lead("marks", 1).over(courseWindow)
        )
        .withColumn(
          "difference_from_previous",
          col("marks") -
            col("previous_marks")
        )

    studentPerformance
      .select(
        "course",
        "student_name",
        "marks",
        "previous_marks",
        "next_marks",
        "difference_from_previous"
      )
      .orderBy(
        "course",
        col("marks").desc
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 14. READ CUSTOMER POLICY DATA
    // ============================================================

    println()
    println("14. READ CUSTOMER POLICY DATA")
    println("------------------------------------------------------------")

    val policiesDF =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/customer_policies.csv")

    policiesDF.show(
      truncate = false
    )


    // ============================================================
    // 15. POLICY SCHEMA
    // ============================================================

    println()
    println("15. POLICY SCHEMA")
    println("------------------------------------------------------------")

    policiesDF.printSchema()


    // ============================================================
    // 16. CUSTOMER POLICY WINDOW
    // ============================================================

    println()
    println("16. CUSTOMER POLICY WINDOW")
    println("------------------------------------------------------------")

    /*
     * Partition by customer.
     *
     * Within each customer:
     *
     * newest policy first.
     */

    val customerPolicyWindow =
      Window
        .partitionBy("customer_id")
        .orderBy(
          col("policy_date").desc,
          col("policy_id").desc
        )


    // ============================================================
    // 17. ROW NUMBER FOR LATEST POLICY
    // ============================================================

    println()
    println("17. LATEST POLICY PER CUSTOMER")
    println("------------------------------------------------------------")

    val policiesWithRowNumber =
      policiesDF.withColumn(
        "row_number",
        row_number().over(customerPolicyWindow)
      )

    policiesWithRowNumber.show(
      truncate = false
    )


    // ============================================================
    // 18. FILTER LATEST POLICY
    // ============================================================

    println()
    println("18. LATEST POLICY RECORD")
    println("------------------------------------------------------------")

    val latestPolicyPerCustomer =
      policiesWithRowNumber
        .filter(
          col("row_number") === 1
        )
        .drop("row_number")
        .orderBy("customer_id")

    latestPolicyPerCustomer.show(
      truncate = false
    )


    // ============================================================
    // 19. LAG POLICY PREMIUM
    // ============================================================

    println()
    println("19. PREVIOUS POLICY PREMIUM")
    println("------------------------------------------------------------")

    val policiesWithPreviousPremium =
      policiesDF.withColumn(
        "previous_premium",
        lag(
          "premium",
          1
        ).over(
          Window
            .partitionBy("customer_id")
            .orderBy(
              col("policy_date")
            )
        )
      )

    policiesWithPreviousPremium
      .orderBy(
        "customer_id",
        "policy_date"
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 20. PREMIUM CHANGE
    // ============================================================

    println()
    println("20. PREMIUM CHANGE")
    println("------------------------------------------------------------")

    val premiumChanges =
      policiesWithPreviousPremium.withColumn(
        "premium_change",
        col("premium") -
          col("previous_premium")
      )

    premiumChanges
      .select(
        "customer_id",
        "customer_name",
        "policy_date",
        "premium",
        "previous_premium",
        "premium_change"
      )
      .orderBy(
        "customer_id",
        "policy_date"
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 21. LEAD POLICY DATE
    // ============================================================

    println()
    println("21. NEXT POLICY DATE")
    println("------------------------------------------------------------")

    val policiesWithNextDate =
      policiesDF.withColumn(
        "next_policy_date",
        lead(
          "policy_date",
          1
        ).over(
          Window
            .partitionBy("customer_id")
            .orderBy(
              col("policy_date")
            )
        )
      )

    policiesWithNextDate
      .orderBy(
        "customer_id",
        "policy_date"
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 22. POLICY SQL VIEW
    // ============================================================

    println()
    println("22. POLICY WINDOW USING SQL")
    println("------------------------------------------------------------")

    policiesDF.createOrReplaceTempView(
      "customer_policies"
    )

    val latestPolicySQL =
      spark.sql(
        """
          SELECT
            policy_id,
            customer_id,
            customer_name,
            policy_type,
            premium,
            policy_status,
            policy_date
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY policy_date DESC, policy_id DESC
              ) AS row_number
            FROM customer_policies
          )
          WHERE row_number = 1
          ORDER BY customer_id
        """
      )

    latestPolicySQL.show(
      truncate = false
    )


    // ============================================================
    // 23. TOP 3 STUDENTS USING SQL
    // ============================================================

    println()
    println("23. TOP 3 STUDENTS PER COURSE USING SQL")
    println("------------------------------------------------------------")

    studentsDF.createOrReplaceTempView(
      "students"
    )

    val top3StudentsSQL =
      spark.sql(
        """
          SELECT
            course,
            student_id,
            student_name,
            marks,
            rank
          FROM (
            SELECT
              *,
              DENSE_RANK() OVER (
                PARTITION BY course
                ORDER BY marks DESC
              ) AS rank
            FROM students
          )
          WHERE rank <= 3
          ORDER BY course, rank, marks DESC
        """
      )

    top3StudentsSQL.show(
      truncate = false
    )


    // ============================================================
    // 24. WINDOW FUNCTION COMPARISON
    // ============================================================

    println()
    println("24. ROW_NUMBER VS RANK VS DENSE_RANK")
    println("------------------------------------------------------------")

    val comparisonWindow =
      Window
        .partitionBy("course")
        .orderBy(
          col("marks").desc
        )

    val rankingComparison =
      studentsDF
        .withColumn(
          "row_number",
          row_number().over(comparisonWindow)
        )
        .withColumn(
          "rank",
          rank().over(comparisonWindow)
        )
        .withColumn(
          "dense_rank",
          dense_rank().over(comparisonWindow)
        )

    rankingComparison
      .select(
        "course",
        "student_name",
        "marks",
        "row_number",
        "rank",
        "dense_rank"
      )
      .orderBy(
        "course",
        col("marks").desc,
        "student_name"
      )
      .show(
        truncate = false
      )


    // ============================================================
    // 25. WINDOW CONCEPTS
    // ============================================================

    println()
    println("25. WINDOW FUNCTION CONCEPTS")
    println("------------------------------------------------------------")

    println(
      """
Window Function
---------------
Performs calculations across related rows while
preserving the original rows.

Window Specification
--------------------
Defines which rows belong to the window and their order.

partitionBy()
-------------
Divides data into independent groups.

orderBy()
---------
Defines ordering inside each partition.

row_number()
------------
Assigns a unique sequential number.

rank()
------
Assigns the same rank to ties and leaves gaps.

dense_rank()
------------
Assigns the same rank to ties without gaps.

lag()
-----
Reads a value from a previous row.

lead()
------
Reads a value from a following row.
"""
    )


    // ============================================================
    // 26. RANKING DIFFERENCE EXAMPLE
    // ============================================================

    println()
    println("26. RANKING DIFFERENCE EXAMPLE")
    println("------------------------------------------------------------")

    println(
      """
Suppose marks are:

95
95
90
85

row_number:
1
2
3
4

rank:
1
1
3
4

dense_rank:
1
1
2
3
"""
    )


    // ============================================================
    // 27. TOP 3 STUDENTS SCENARIO
    // ============================================================

    println()
    println("27. TOP 3 STUDENTS SCENARIO")
    println("------------------------------------------------------------")

    println(
      """
Requirement:

Find the top 3 students in every course.

Solution:

1. Partition students by course.
2. Order each course by marks descending.
3. Apply ranking.
4. Filter rank <= 3.

Pipeline:

Students
   |
   v
partitionBy(course)
   |
   v
orderBy(marks DESC)
   |
   v
dense_rank()
   |
   v
rank <= 3
   |
   v
Top 3 Students per Course
"""
    )


    // ============================================================
    // 28. LATEST POLICY SCENARIO
    // ============================================================

    println()
    println("28. LATEST POLICY SCENARIO")
    println("------------------------------------------------------------")

    println(
      """
Requirement:

Find the latest policy for every customer.

Solution:

1. Partition by customer_id.
2. Order by policy_date descending.
3. Apply row_number().
4. Keep row_number = 1.

Pipeline:

Policies
   |
   v
partitionBy(customer_id)
   |
   v
orderBy(policy_date DESC)
   |
   v
row_number()
   |
   v
row_number = 1
   |
   v
Latest Policy per Customer
"""
    )


    // ============================================================
    // 29. FINAL SUMMARY
    // ============================================================

    println()
    println("============================================================")
    println("DAY 17 COMPLETED")
    println("============================================================")

    println()
    println("Completed:")
    println("1. row_number()")
    println("2. rank()")
    println("3. dense_rank()")
    println("4. partitionBy()")
    println("5. orderBy() inside windows")
    println("6. Top 3 students per course")
    println("7. Latest policy per customer")
    println("8. lag()")
    println("9. lead()")
    println("10. Previous-value comparison")
    println("11. Next-value comparison")
    println("12. Window functions using SQL")

    println()
    println(
      "Key concept: window functions calculate information across " +
      "related rows without collapsing the original rows."
    )

    println()
    println("============================================================")


    // ============================================================
    // STOP SPARK
    // ============================================================

    spark.stop()
  }
}
