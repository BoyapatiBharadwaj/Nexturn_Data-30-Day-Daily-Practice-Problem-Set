import org.apache.spark.sql.SparkSession

object Day3SparkApp {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // DAY 3 - SPARK SETUP AND FIRST APPLICATION
    // ============================================================

    val cores =
      if (args.nonEmpty) args(0)
      else "2"

    val master = s"local[$cores]"

    // ============================================================
    // CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession
      .builder()
      .appName("Day 3 - Spark First Application")
      .master(master)
      .getOrCreate()

    // ============================================================
    // GET SPARK CONTEXT
    // ============================================================

    val sc = spark.sparkContext

    // Reduce Spark's console logging so our output is easy to read.
    sc.setLogLevel("WARN")

    // ============================================================
    // APPLICATION INFORMATION
    // ============================================================

    println()
    println("============================================================")
    println("             DAY 3 - SPARK FIRST APPLICATION")
    println("============================================================")

    println()
    println("SPARK CONFIGURATION")
    println("------------------------------------------------------------")
    println(s"Spark Version       : ${spark.version}")
    println(s"Application Name    : ${sc.appName}")
    println(s"Master              : ${sc.master}")
    println(s"Application ID      : ${sc.applicationId}")
    println(s"Default Parallelism : ${sc.defaultParallelism}")
    println(s"Configured Cores    : $cores")

    // ============================================================
    // SPARK ARCHITECTURE
    // ============================================================

    println()
    println("SPARK ARCHITECTURE")
    println("------------------------------------------------------------")
    println("Driver")
    println("  -> Runs the main application")
    println("  -> Creates SparkSession and SparkContext")
    println("  -> Coordinates Spark jobs")

    println()
    println("Executor")
    println("  -> Executes Spark tasks")
    println("  -> Processes data")
    println("  -> Stores intermediate data when required")

    println()
    println("Cluster Manager")
    println("  -> Allocates resources to Spark applications")
    println("  -> Examples: Standalone, YARN, Kubernetes")

    println()
    println(s"Current Execution Mode: $master")

    // ============================================================
    // READ TEXT FILE
    // ============================================================

    println()
    println("READING TEXT FILE")
    println("------------------------------------------------------------")

    val inputPath = "data/input.txt"

    val lines = sc.textFile(inputPath)

    println(s"Input File           : $inputPath")
    println(s"Number of Partitions : ${lines.getNumPartitions}")

    // ============================================================
    // DISPLAY FILE CONTENT
    // ============================================================

    val lineCount = lines.count()

    println(s"Number of Lines      : $lineCount")

    println()
    println("FILE CONTENTS")
    println("------------------------------------------------------------")

    lines.collect().zipWithIndex.foreach {
      case (line, index) =>
        println(f"${index + 1}%2d | $line")
    }

    // ============================================================
    // SIMPLE SPARK TRANSFORMATION
    // ============================================================

    println()
    println("SIMPLE SPARK PROCESSING")
    println("------------------------------------------------------------")

    val sparkLines = lines.filter(
      line => line.toLowerCase.contains("spark")
    )

    val sparkLineCount = sparkLines.count()

    println(s"Lines containing 'Spark': $sparkLineCount")

    println()
    println("Matching Lines:")
    println("------------------------------------------------------------")

    sparkLines.collect().foreach { line =>
      println(s"-> $line")
    }

    // ============================================================
    // DEMONSTRATE AN RDD TRANSFORMATION
    // ============================================================

    println()
    println("RDD INFORMATION")
    println("------------------------------------------------------------")

    println(s"RDD Type              : ${lines.getClass.getSimpleName}")
    println(s"RDD Partitions        : ${lines.getNumPartitions}")
    println("Transformation        : filter")
    println("Actions               : count, collect")

    // ============================================================
    // LOCAL MODE COMPARISON
    // ============================================================

    println()
    println("LOCAL MODE")
    println("------------------------------------------------------------")

    if (cores == "2") {
      println("This application is currently using local[2].")
      println("Spark can execute tasks using 2 local worker threads.")
    } else if (cores == "4") {
      println("This application is currently using local[4].")
      println("Spark can execute tasks using 4 local worker threads.")
    } else {
      println(s"This application is currently using local[$cores].")
    }

    // ============================================================
    // DAY 3 CHECKLIST
    // ============================================================

    println()
    println("DAY 3 REQUIREMENTS")
    println("------------------------------------------------------------")
    println("[PASS] Scala Spark project created with sbt")
    println("[PASS] SparkSession created")
    println("[PASS] SparkContext obtained")
    println("[PASS] Text file read successfully")
    println("[PASS] File contents displayed")
    println("[PASS] Driver explained")
    println("[PASS] Executor explained")
    println("[PASS] Cluster Manager explained")
    println("[PASS] Local execution configured")
    println(s"[PASS] Application running with $cores core(s)")

    println()
    println("============================================================")
    println("             DAY 3 APPLICATION COMPLETE")
    println("============================================================")
    println()

    // ============================================================
    // CLEAN SHUTDOWN
    // ============================================================

    spark.stop()
  }
}
