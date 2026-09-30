import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day21SparkCatalogApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Day21 - Spark Catalog")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("=" * 90)
    println("DAY 21 - SPARK CATALOG")
    println("=" * 90)
    println()


    // ============================================================
    // 2. LIST DATABASES
    // ============================================================

    println("=" * 90)
    println("1. LIST DATABASES")
    println("=" * 90)

    println("Databases available before creating hotel_analytics:")

    spark.catalog
      .listDatabases()
      .show(false)


    // ============================================================
    // 3. CREATE DATABASE
    // ============================================================

    println()
    println("=" * 90)
    println("2. CREATE ANALYTICS DATABASE")
    println("=" * 90)

    spark.sql(
      """
        |CREATE DATABASE IF NOT EXISTS hotel_analytics
        |"""
        .stripMargin
    )

    println("Database hotel_analytics created or already exists.")


    // ============================================================
    // 4. LIST DATABASES AGAIN
    // ============================================================

    println()
    println("=" * 90)
    println("3. DATABASES AFTER CREATION")
    println("=" * 90)

    spark.catalog
      .listDatabases()
      .show(false)


    // ============================================================
    // 5. USE DATABASE
    // ============================================================

    println()
    println("=" * 90)
    println("4. USE HOTEL ANALYTICS DATABASE")
    println("=" * 90)

    spark.sql(
      """
        |USE hotel_analytics
        |"""
        .stripMargin
    )

    println("Current database:")

    println(
      spark.catalog.currentDatabase
    )


    // ============================================================
    // 6. READ HOTEL BOOKING CSV
    // ============================================================

    println()
    println("=" * 90)
    println("5. READ HOTEL BOOKINGS")
    println("=" * 90)

    val bookings =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/hotel_bookings.csv")

    bookings.show(20, false)

    println("Schema:")

    bookings.printSchema()


    // ============================================================
    // 7. CREATE TEMPORARY VIEW
    // ============================================================

    println()
    println("=" * 90)
    println("6. CREATE TEMPORARY VIEW")
    println("=" * 90)

    bookings.createOrReplaceTempView(
      "hotel_bookings_temp"
    )

    println(
      "Temporary view hotel_bookings_temp created."
    )


    // ============================================================
    // 8. LIST TABLES AND VIEWS
    // ============================================================

    println()
    println("=" * 90)
    println("7. LIST TABLES AND VIEWS")
    println("=" * 90)

    spark.catalog
      .listTables()
      .show(false)


    // ============================================================
    // 9. QUERY TEMPORARY VIEW
    // ============================================================

    println()
    println("=" * 90)
    println("8. QUERY TEMPORARY VIEW")
    println("=" * 90)

    val confirmedBookings =
      spark.sql(
        """
          |SELECT
          |    booking_id,
          |    customer_name,
          |    hotel_name,
          |    city,
          |    room_type,
          |    nights,
          |    booking_amount
          |FROM hotel_bookings_temp
          |WHERE booking_status = 'Confirmed'
          |ORDER BY booking_amount DESC
          |"""
          .stripMargin
      )

    confirmedBookings.show(100, false)


    // ============================================================
    // 10. CREATE MANAGED TABLE
    // ============================================================

    println()
    println("=" * 90)
    println("9. CREATE MANAGED TABLE")
    println("=" * 90)

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS hotel_bookings
        |USING PARQUET
        |AS
        |SELECT *
        |FROM hotel_bookings_temp
        |"""
        .stripMargin
    )

    println(
      "Managed table hotel_bookings created."
    )


    // ============================================================
    // 11. LIST TABLES
    // ============================================================

    println()
    println("=" * 90)
    println("10. LIST TABLES")
    println("=" * 90)

    spark.catalog
      .listTables()
      .show(false)


    // ============================================================
    // 12. SHOW TABLES USING SQL
    // ============================================================

    println()
    println("=" * 90)
    println("11. SHOW TABLES")
    println("=" * 90)

    spark.sql(
      """
        |SHOW TABLES
        |"""
        .stripMargin
    ).show(false)


    // ============================================================
    // 13. DESCRIBE TABLE
    // ============================================================

    println()
    println("=" * 90)
    println("12. DESCRIBE TABLE")
    println("=" * 90)

    spark.sql(
      """
        |DESCRIBE hotel_bookings
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 14. DESCRIBE EXTENDED
    // ============================================================

    println()
    println("=" * 90)
    println("13. DESCRIBE EXTENDED")
    println("=" * 90)

    spark.sql(
      """
        |DESCRIBE EXTENDED hotel_bookings
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 15. CATALOG TABLE METADATA
    // ============================================================

    println()
    println("=" * 90)
    println("14. TABLE METADATA USING CATALOG API")
    println("=" * 90)

    val tableMetadata =
      spark.catalog
        .getTable("hotel_bookings")

    println(
      s"Table name      : ${tableMetadata.name}"
    )

    println(
      s"Database        : ${tableMetadata.database}"
    )

    println(
      s"Table type      : ${tableMetadata.tableType}"
    )

    println(
      s"Is temporary    : ${tableMetadata.isTemporary}"
    )

    println(
      s"Catalog         : ${tableMetadata.catalog}"
    )


    // ============================================================
    // 16. TABLE EXISTS CHECK
    // ============================================================

    println()
    println("=" * 90)
    println("15. TABLE EXISTS CHECK")
    println("=" * 90)

    println(
      s"hotel_bookings exists = ${
        spark.catalog.tableExists("hotel_bookings")
      }"
    )

    println(
      s"unknown_table exists = ${
        spark.catalog.tableExists("unknown_table")
      }"
    )


    // ============================================================
    // 17. COLUMN METADATA
    // ============================================================

    println()
    println("=" * 90)
    println("16. COLUMN METADATA")
    println("=" * 90)

    spark.catalog
      .listColumns("hotel_bookings")
      .show(100, false)


    // ============================================================
    // 18. QUERY REGISTERED TABLE
    // ============================================================

    println()
    println("=" * 90)
    println("17. QUERY REGISTERED TABLE")
    println("=" * 90)

    val bookingTable =
      spark.table("hotel_bookings")

    bookingTable
      .select(
        "booking_id",
        "customer_name",
        "hotel_name",
        "city",
        "booking_amount",
        "booking_status"
      )
      .show(20, false)


    // ============================================================
    // 19. HOTEL REVENUE ANALYSIS
    // ============================================================

    println()
    println("=" * 90)
    println("18. HOTEL REVENUE ANALYSIS")
    println("=" * 90)

    spark.sql(
      """
        |SELECT
        |    hotel_name,
        |    city,
        |    COUNT(*) AS booking_count,
        |    SUM(booking_amount) AS total_revenue,
        |    AVG(booking_amount) AS average_booking
        |FROM hotel_bookings
        |GROUP BY hotel_name, city
        |ORDER BY total_revenue DESC
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 20. CITY ANALYSIS
    // ============================================================

    println()
    println("=" * 90)
    println("19. CITY ANALYSIS")
    println("=" * 90)

    spark.sql(
      """
        |SELECT
        |    city,
        |    COUNT(*) AS bookings,
        |    SUM(nights) AS total_nights,
        |    SUM(booking_amount) AS revenue
        |FROM hotel_bookings
        |GROUP BY city
        |ORDER BY revenue DESC
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 21. ROOM TYPE ANALYSIS
    // ============================================================

    println()
    println("=" * 90)
    println("20. ROOM TYPE ANALYSIS")
    println("=" * 90)

    spark.sql(
      """
        |SELECT
        |    room_type,
        |    COUNT(*) AS booking_count,
        |    SUM(nights) AS total_nights,
        |    SUM(booking_amount) AS total_revenue,
        |    AVG(booking_amount) AS average_booking
        |FROM hotel_bookings
        |GROUP BY room_type
        |ORDER BY total_revenue DESC
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 22. BOOKING STATUS ANALYSIS
    // ============================================================

    println()
    println("=" * 90)
    println("21. BOOKING STATUS ANALYSIS")
    println("=" * 90)

    spark.sql(
      """
        |SELECT
        |    booking_status,
        |    COUNT(*) AS booking_count,
        |    SUM(booking_amount) AS total_amount
        |FROM hotel_bookings
        |GROUP BY booking_status
        |ORDER BY booking_count DESC
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 23. HIGH-VALUE BOOKINGS
    // ============================================================

    println()
    println("=" * 90)
    println("22. HIGH-VALUE BOOKINGS")
    println("=" * 90)

    spark.sql(
      """
        |SELECT
        |    booking_id,
        |    customer_name,
        |    hotel_name,
        |    city,
        |    room_type,
        |    nights,
        |    booking_amount
        |FROM hotel_bookings
        |WHERE booking_amount >= 20000
        |ORDER BY booking_amount DESC
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 24. CREATE SECOND TABLE
    // ============================================================

    println()
    println("=" * 90)
    println("23. CREATE HOTEL MASTER TABLE")
    println("=" * 90)

    val hotelMaster =
      bookings
        .select(
          "hotel_id",
          "hotel_name",
          "city"
        )
        .distinct()

    hotelMaster.createOrReplaceTempView(
      "hotel_master_temp"
    )

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS hotel_master
        |USING PARQUET
        |AS
        |SELECT *
        |FROM hotel_master_temp
        |"""
        .stripMargin
    )

    println(
      "hotel_master table created."
    )


    // ============================================================
    // 25. LIST ALL TABLES
    // ============================================================

    println()
    println("=" * 90)
    println("24. ALL TABLES IN HOTEL ANALYTICS")
    println("=" * 90)

    spark.catalog
      .listTables("hotel_analytics")
      .show(false)


    // ============================================================
    // 26. JOIN TABLES
    // ============================================================

    println()
    println("=" * 90)
    println("25. JOIN BOOKINGS WITH HOTEL MASTER")
    println("=" * 90)

    spark.sql(
      """
        |SELECT
        |    b.booking_id,
        |    b.customer_name,
        |    b.hotel_name,
        |    b.city,
        |    b.room_type,
        |    b.booking_amount,
        |    h.hotel_id
        |FROM hotel_bookings b
        |JOIN hotel_master h
        |    ON b.hotel_id = h.hotel_id
        |ORDER BY b.booking_id
        |"""
        .stripMargin
    ).show(100, false)


    // ============================================================
    // 27. DATABASE METADATA
    // ============================================================

    println()
    println("=" * 90)
    println("26. DATABASE METADATA")
    println("=" * 90)

    val databaseMetadata =
      spark.catalog
        .getDatabase("hotel_analytics")

    println(
      s"Database name      : ${databaseMetadata.name}"
    )

    println(
      s"Catalog             : ${databaseMetadata.catalog}"
    )

    println(
      s"Description         : ${databaseMetadata.description}"
    )

    println(
      s"Location            : ${databaseMetadata.locationUri}"
    )


    // ============================================================
    // 28. TEMPORARY VIEW VS TABLE
    // ============================================================

    println()
    println("=" * 90)
    println("27. TEMPORARY VIEW VS REGISTERED TABLE")
    println("=" * 90)

    println(
      """
        |TEMPORARY VIEW
        |--------------
        |
        |hotel_bookings_temp
        |
        |Exists only for the current SparkSession.
        |
        |It does not represent a persistent table in storage.
        |
        |
        |REGISTERED TABLE
        |----------------
        |
        |hotel_bookings
        |
        |Registered in the Spark catalog.
        |
        |The table can be associated with persisted data.
        |
        |
        |In this project:
        |
        |hotel_bookings_temp -> temporary view
        |
        |hotel_bookings       -> managed table
        |"""
        .stripMargin
    )


    // ============================================================
    // 29. CREATE GLOBAL TEMP VIEW
    // ============================================================

    println()
    println("=" * 90)
    println("28. GLOBAL TEMPORARY VIEW")
    println("=" * 90)

    bookings.createOrReplaceGlobalTempView(
      "global_hotel_bookings"
    )

    println(
      """
        |Global temporary views are available using:
        |
        |global_temp.view_name
        |
        |Example:
        |
        |SELECT *
        |FROM global_temp.global_hotel_bookings
        |"""
        .stripMargin
    )

    spark.sql(
      """
        |SELECT
        |    booking_id,
        |    customer_name,
        |    hotel_name,
        |    booking_amount
        |FROM global_temp.global_hotel_bookings
        |ORDER BY booking_amount DESC
        |"""
        .stripMargin
    ).show(10, false)


    // ============================================================
    // 30. CATALOG FUNCTION EXAMPLES
    // ============================================================

    println()
    println("=" * 90)
    println("29. SPARK CATALOG API")
    println("=" * 90)

    println(
      """
        |Useful Catalog APIs:
        |
        |spark.catalog.listDatabases()
        |
        |spark.catalog.listTables()
        |
        |spark.catalog.listColumns("table")
        |
        |spark.catalog.getTable("table")
        |
        |spark.catalog.getDatabase("database")
        |
        |spark.catalog.tableExists("table")
        |
        |spark.catalog.currentDatabase
        |"""
        .stripMargin
    )


    // ============================================================
    // 31. FINAL ANALYTICS REPORT
    // ============================================================

    println()
    println("=" * 90)
    println("30. FINAL HOTEL ANALYTICS REPORT")
    println("=" * 90)

    val finalReport =
      spark.sql(
        """
          |SELECT
          |    city,
          |    COUNT(*) AS total_bookings,
          |    SUM(nights) AS total_nights,
          |    SUM(booking_amount) AS total_revenue,
          |    ROUND(
          |        AVG(booking_amount),
          |        2
          |    ) AS average_booking_value
          |FROM hotel_bookings
          |WHERE booking_status != 'Cancelled'
          |GROUP BY city
          |ORDER BY total_revenue DESC
          |"""
          .stripMargin
      )

    finalReport.show(100, false)


    // ============================================================
    // 32. VIVA SUMMARY
    // ============================================================

    println()
    println("=" * 90)
    println("31. DAY 21 VIVA SUMMARY")
    println("=" * 90)

    println(
      """
        |Q1. What is Spark Catalog?
        |
        |A:
        |Spark Catalog provides an interface for discovering
        |and managing databases, tables, views and metadata
        |available to Spark.
        |
        |
        |Q2. How do you list databases?
        |
        |A:
        |
        |spark.catalog.listDatabases()
        |
        |
        |Q3. How do you list tables?
        |
        |A:
        |
        |spark.catalog.listTables()
        |
        |
        |Q4. How do you create a temporary view?
        |
        |A:
        |
        |df.createOrReplaceTempView("view_name")
        |
        |
        |Q5. How do you query a temporary view?
        |
        |A:
        |
        |spark.sql("SELECT * FROM view_name")
        |
        |
        |Q6. What is a registered table?
        |
        |A:
        |A table known to Spark's catalog and associated with
        |table metadata and storage information.
        |
        |
        |Q7. How do you inspect a table schema?
        |
        |A:
        |
        |DESCRIBE table_name
        |
        |or:
        |
        |df.printSchema()
        |
        |
        |Q8. How do you inspect detailed table metadata?
        |
        |A:
        |
        |DESCRIBE EXTENDED table_name
        |
        |
        |Q9. Difference between temporary view and table?
        |
        |A:
        |A temporary view is scoped to the SparkSession and is
        |not a persistent table. A registered table has catalog
        |metadata and can reference persisted data.
        |
        |
        |Q10. What is a global temporary view?
        |
        |A:
        |A temporary view available across SparkSessions within
        |the application through the global_temp database.
        |
        |
        |Q11. How do you check whether a table exists?
        |
        |A:
        |
        |spark.catalog.tableExists("table_name")
        |
        |
        |Q12. Why is a catalog important in data engineering?
        |
        |A:
        |It provides metadata and discovery information so
        |datasets can be organized and queried as databases,
        |tables and views instead of treating every dataset
        |only as an individual file path.
        |"""
        .stripMargin
    )


    // ============================================================
    // 33. FINAL SUMMARY
    // ============================================================

    println()
    println("=" * 90)
    println("DAY 21 COMPLETED")
    println("=" * 90)

    println(
      """
        |Topics completed:
        |
        |[✓] Spark Catalog
        |[✓] List databases
        |[✓] Create database
        |[✓] List tables
        |[✓] Temporary views
        |[✓] Global temporary views
        |[✓] Managed table
        |[✓] Query registered tables
        |[✓] DESCRIBE
        |[✓] DESCRIBE EXTENDED
        |[✓] Table metadata
        |[✓] Column metadata
        |[✓] Database metadata
        |[✓] Table existence checks
        |[✓] Hotel booking analytics
        |"""
        .stripMargin
    )

    println()

    spark.stop()
  }
}
