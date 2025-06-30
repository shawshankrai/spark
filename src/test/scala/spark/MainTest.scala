package spark

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import utils.SparkSessionProvider
import job.{JobRunner, ExampleJob}

/**
 * Unit tests for Main object logic.
 */
class MainTest extends AnyFunSuite {

  test("DataFrame should contain expected messages") {
    val spark = SparkSessionProvider.getSession("MainTest")
    import spark.implicits._
    val data = Seq("Hello, World!", "Welcome to Spark with Scala.")
    val df = data.toDF("message")
    val messages = df.collect().map(_.getString(0)).toSet
    assert(messages.contains("Hello, World!"))
    assert(messages.contains("Welcome to Spark with Scala."))
    spark.stop()
  }

  test("Main job runs without error") {
    val spark = SparkSessionProvider.getSession()
    noException should be thrownBy {
      new JobRunner(spark, ExampleJob).run()
    }
    spark.stop()
  }
} 