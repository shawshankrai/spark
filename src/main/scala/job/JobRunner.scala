package job

import org.apache.spark.sql.SparkSession

/**
 * Encapsulates the main Spark job logic.
 * @param spark The SparkSession to use for the job.
 */
class JobRunner(spark: SparkSession, job: Job) {
  /**
   * Runs the main job logic: creates a DataFrame and shows it.
   */
  def run(): Unit = {
    job.run(spark)
  }
} 