object Day1 {

  def main(args: Array[String]): Unit = {

    println("===== DAY 1 - SCALA ESSENTIALS =====")

    // ----------------------------------------
    // 1. val, var and lazy val
    // ----------------------------------------

    val college = "ABC College"
    var age = 20

    println(s"College: $college")
    println(s"Initial age: $age")

    age = 21

    println(s"Updated age: $age")

    lazy val message = {
      println("lazy val is being evaluated now")
      "Welcome to Scala"
    }

    println("Before accessing lazy val")
    println(message)
    println(message)


    // ----------------------------------------
    // 2. Immutable collections
    // ----------------------------------------

    val numbers = List(10, 20, 30, 40, 50)

    println(s"Original list: $numbers")

    val doubledNumbers = numbers.map(number => number * 2)

    println(s"Doubled list: $doubledNumbers")
    println(s"Original list still unchanged: $numbers")


    // ----------------------------------------
    // 3. List, Vector, Set and Map
    // ----------------------------------------

    val studentList = List("Rahul", "Priya", "Arun", "Rahul")

    val studentVector = Vector("Rahul", "Priya", "Arun", "Rahul")

    val studentSet = Set("Rahul", "Priya", "Arun", "Rahul")

    val studentMap = Map(
      "Rahul" -> 85,
      "Priya" -> 92,
      "Arun" -> 76
    )

    println(s"List: $studentList")
    println(s"Vector: $studentVector")
    println(s"Set: $studentSet")
    println(s"Map: $studentMap")


    // ----------------------------------------
    // 4. for-comprehension with yield
    // ----------------------------------------

    val students = List(
      ("Rahul", 85),
      ("Priya", 92),
      ("Arun", 76)
    )

    val studentResults = for {
      (name, marks) <- students
      if marks >= 80
    } yield {
      s"$name scored $marks"
    }

    println("\nStudents with marks >= 80:")

    studentResults.foreach(println)


    // ----------------------------------------
    // 5. Logger trait
    // ----------------------------------------

    val consoleLogger = new ConsoleLogger
    val fileLogger = new FileLogger

    consoleLogger.log("Student processing started")
    fileLogger.log("Student processing completed")


    // ----------------------------------------
    // 6. Student Grade Processor
    // ----------------------------------------

    val studentMarks = List(
      Student("Rahul", List(85, 90, 80)),
      Student("Priya", List(92, 88, 95)),
      Student("Arun", List(60, 65, 70)),
      Student("Sneha", List(40, 45, 50)),
      Student("Kiran", List(75, 80, 78))
    )

    println("\n===== STUDENT GRADE PROCESSOR =====")

    val results = studentMarks.map { student =>

      val total = student.marks.sum

      val average = student.marks.sum.toDouble / student.marks.size

      val grade =
        if (average >= 90) "A"
        else if (average >= 80) "B"
        else if (average >= 70) "C"
        else if (average >= 60) "D"
        else "F"

      StudentResult(
        student.name,
        total,
        average,
        grade
      )
    }

    results.foreach { result =>
      println(
        f"${result.name}%-10s Total: ${result.total}%3d Average: ${result.average}%5.2f Grade: ${result.grade}"
      )
    }


    // ----------------------------------------
    // 7. Find passed students
    // ----------------------------------------

    val passedStudents = results.filter(_.grade != "F")

    println("\n===== PASSED STUDENTS =====")

    passedStudents.foreach { student =>
      println(s"${student.name} -> Grade ${student.grade}")
    }


    // ----------------------------------------
    // 8. Find top student
    // ----------------------------------------

    val topStudent = results.maxBy(_.average)

    println("\n===== TOP STUDENT =====")

    println(
      f"${topStudent.name} with average ${topStudent.average}%.2f"
    )
  }
}


// ----------------------------------------
// Student case class
// ----------------------------------------

case class Student(
  name: String,
  marks: List[Int]
)


// ----------------------------------------
// Student result case class
// ----------------------------------------

case class StudentResult(
  name: String,
  total: Int,
  average: Double,
  grade: String
)


// ----------------------------------------
// Logger trait
// ----------------------------------------

trait Logger {

  def log(message: String): Unit
}


// ----------------------------------------
// First implementation
// ----------------------------------------

class ConsoleLogger extends Logger {

  override def log(message: String): Unit = {
    println(s"[CONSOLE] $message")
  }
}


// ----------------------------------------
// Second implementation
// ----------------------------------------

class FileLogger extends Logger {

  override def log(message: String): Unit = {
    println(s"[FILE] $message")
  }
}
