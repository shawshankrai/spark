package job

import org.apache.spark.sql.SparkSession

object ExampleJob extends Job {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val data = Seq("Hello, World!", "Welcome to Spark with Scala.")
    val df = data.toDF("message")
    df.show()
  }
} 