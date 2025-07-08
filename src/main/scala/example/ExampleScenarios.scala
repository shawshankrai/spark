package example

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window

import job.Job

object ExampleScenarios extends Job {
  def run(spark: SparkSession): Unit = {
    
    import spark.implicits._  // import implicits for DataFrame operations

    1. Create a DataFrame and show it
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

    // Step 11: Window functions
    // Q21: What are window functions in Spark SQL? Give an example use case.
    // Q22: How do you use row_number, rank, or dense_rank in Spark DataFrames?
    // Q23: How do you define a window specification and apply it to a DataFrame?

    val data  = Seq(
      ("Alice", "Sales", 5000),
      ("Bob", "Sales", 4800),
      ("Charlie", "Sales", 5200),
      ("David", "HR", 3900),
      ("Eve", "HR", 4000)
    )

    val df = data.toDF("name", "department", "salary")
    df.show()

    // Suppose we want to rank employees within each department by salary
    val windowSpec = Window.partitionBy("department").orderBy(desc("salary"))

    // Apply Window Functions
    val rankedDF = df.withColumn("rank", rank().over(windowSpec))
    .withColumn("row_nmuber", row_number().over(windowSpec))

    rankedDF.show()

    val sumDF = df.withColumn("sum_salary", sum("salary").over(windowSpec))
    sumDF.show()

    val withLeadLag = df.withColumn("lead_salary", lead("salary", 1).over(windowSpec))
    .withColumn("lag_salary", lag("salary", 1).over(windowSpec))

    withLeadLag.show()

    val withCustomWindow = df.withColumn("preceding", sum("salary").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    .withColumn("following", sum("salary").over(windowSpec.rowsBetween(Window.currentRow, Window.unboundedFollowing)))

    withCustomWindow.show()

    // Step 12: Custom partitioning and shuffles
    // Q24: How do you implement custom partitioning in Spark?
    // Q25: What is the difference between hash partitioning and range partitioning?
    // Q26: How do you minimize shuffle operations in Spark jobs?

    val rddToDf = spark.sparkContext.parallelize(Seq(("Alice", "Sales", 5000), ("Bob", "Sales", 4800), ("Charlie", "Sales", 5200), ("David", "HR", 3900), ("Eve", "HR", 4000)))
    .toDF("name", "department", "salary")

    println(s"Original partition nums: ${rddToDf.rdd.getNumPartitions}")

    // Repartition by column (department)
    val repartitionedByDept = rddToDf.repartition($"department")
    println(s"Partition nums after repartition by department: ${repartitionedByDept.rdd.getNumPartitions}")

    // Repartition by number of partitions (e.g., 3)
    val repartitionedByNum = rddToDf.repartition(3)
    println(s"Partition nums after repartition to 3: ${repartitionedByNum.rdd.getNumPartitions}")
    
    rddToDf.show()

    // Step 13: Checkpointing and fault tolerance
    // Q27: What is checkpointing in Spark and when should you use it?
    // Q28: How does Spark handle lineage and DAG recovery after a failure?
    // Q29: How do you enable and use checkpointing in a Spark application?

    // Step 14: Structured Streaming
    // Q30: What is Structured Streaming in Spark and how does it differ from classic streaming?
    // Q31: How do you define a streaming DataFrame and write a streaming query?
    // Q32: What are output modes in Structured Streaming and when would you use each?
    // Q33: How does watermarking work in Spark Structured Streaming?

    // Step 15: Advanced joins and optimizations
    // Q34: What is a map-side join and when is it beneficial?
    // Q35: How do you optimize skewed joins in Spark?
    // Q36: What is a sort-merge join and when does Spark use it?
    // Q37: How do you control broadcast join thresholds in Spark SQL?

    // Step 16: Performance tuning and internals
    // Q38: What is Tungsten and how does it improve Spark performance?
    // Q39: What is the Catalyst optimizer and what kinds of optimizations does it perform?
    // Q40: How do you tune Spark executor memory and core settings for a large job?
    // Q41: What is the difference between narrow and wide dependencies in Spark?
    // Q42: How does Spark handle task scheduling and speculative execution?
  }
} 