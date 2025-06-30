package glue

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import job.{JobRunner, ExampleJob}

/**
 * Entry point for AWS Glue jobs.
 * Sets up GlueContext and delegates to JobRunner.
 */
object GlueApp {
  /**
   * Main method for Glue job execution.
   */
  def main(args: Array[String]): Unit = {
    val sparkContext: SparkContext = SparkSession.builder().getOrCreate().sparkContext
    val glueContext = new GlueContext(sparkContext)
    val sparkSession = glueContext.getSparkSession
    new JobRunner(sparkSession, ExampleJob).run()
    sparkContext.stop()
  }
} 