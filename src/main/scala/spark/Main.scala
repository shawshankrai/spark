package spark

import utils.SparkSessionProvider
import job.{JobRunner, ExampleJob}
import org.apache.spark.sql.SparkSession

/**
 * Main entry point for the Spark application.
 */
object Main {
  /**
   * Application entry point.
   * Sets up SparkSession and delegates to JobRunner.
   */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionProvider.getSession()
    new JobRunner(spark, ExampleJob).run()
    spark.stop()
  }
} 