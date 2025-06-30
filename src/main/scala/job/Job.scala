package job

import org.apache.spark.sql.SparkSession

/**
 * Trait representing a pluggable Spark job.
 *
 * Implement this trait to define custom job logic that can be run with a SparkSession.
 */
trait Job {
  /**
   * Run the job logic using the provided SparkSession.
   *
   * @param spark The SparkSession to use for executing the job.
   */
  def run(spark: SparkSession): Unit
} 