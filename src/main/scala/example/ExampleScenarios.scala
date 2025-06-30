package example

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import job.Job

object ExampleScenarios extends Job {
  def run(spark: SparkSession): Unit = {
    
    import spark.implicits._  // import implicits for DataFrame operations

    // 1. Create a DataFrame and show it
    val data = Seq(("John", 25), ("Jane", 30), ("Jim", 35))
    val df = data.toDF("name", "age")
    df.show()

    // 2. Filter and select columns
    // Step 1: Create a DataFrame and show it
    // Q1: What is a DataFrame in Spark?
    // Q2: How do you create a DataFrame from a Scala sequence?
    // Q3: What does df.show() do?
    val dfFiltered = df.filter($"age" > 25)
    dfFiltered.show()

    // Step 2: Filter and select columns
    // Q4: How do you filter rows in a DataFrame?
    // Q5: How do you select specific columns?
    val dfSelected = df.select($"name")
    dfSelected.show()

    // Step 3: GroupBy and aggregation
    // Q6: How do you group data and count occurrences?
    // Q7: What is the difference between groupBy and agg?

    val people = spark.read.option("header", true).option("inferSchema", true).csv("src/main/resources/people.csv")
    people.show()


    val peopleGroupedSalary = people.groupBy($"age").agg(sum($"salary"))
    peopleGroupedSalary.show()

    // Step 4: Register as SQL view and run SQL
    // Q8: How do you register a DataFrame as a SQL view?
    // Q9: How do you run a SQL query in Spark?

    people.createOrReplaceTempView("people")
    val peopleGroupedBySalary = spark.sql("SELECT sum(salary) as total_salary from people group by age")
    peopleGroupedBySalary.show()

    // Step 5: Define and use a UDF
    // Q10: What is a UDF and when would you use it?
    // Q11: How do you add a new column using a UDF?
    val isHigher = (x: String, y: Double) => x.toDouble > y
    val isHigherEarner = udf(isHigher)

    val dfWithFlag = people.withColumn("is_higher_than_100000", isHigherEarner($"salary", lit(100000.0)))

    // Step 6: Partitioning and caching
    // Q12: How do you repartition a DataFrame?
    // Q13: What does cache() do and when should you use it?
    dfWithFlag.repartition($"is_higher_than_100000")
    dfWithFlag.cache()
    dfWithFlag.count()
    dfWithFlag.show()
    println(dfWithFlag.rdd.getNumPartitions)

    // Step 7: Join example
    // Q14: How do you join two DataFrames?
    // Q15: What is a left_outer join?
    val departments = spark.read.option("header", true).option("inferSchema", true).csv("src/main/resources/departments.csv")

    val employee = people.join(departments, Seq("dept_id"), "left").drop(departments("city"))
    employee.show()

    // Step 8: Reading/writing data (CSV)
    // Q16: How do you write a DataFrame to CSV?
    // Q17: How do you read a CSV file as a DataFrame?
    employee.write.mode("overwrite").parquet("src/main/resources/result")

    // Step 9: Data skew scenario
    // Q18: What is data skew and why is it a problem?
    // Q19: How can you handle data skew in Spark?
    val employeeWithDept = people.join(broadcast(departments), Seq("dept_id"), "left") // broadcast the look up table
    employeeWithDept.show()

    // Step 10: Error handling
    // Q20: How do you handle errors in Spark jobs?
    try {
        val parquetFile = spark.read.parquet("src/main/resources/result")
        parquetFile.show()
    } catch {
        case e: Exception => println(s"Error ${e.getMessage}")
    }
  }
} 