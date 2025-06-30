package utils

import org.apache.spark.sql.SparkSession

/**
 * Provides a configured SparkSession instance for the application.
 */
object SparkSessionProvider {
  /**
   * Returns a SparkSession with the given app name and local master.
   */
  def getSession(appName: String = "Spark Hello World"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
} 