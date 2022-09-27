package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSparkSession(appName: String, mode: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config("spark.master", mode)
      .getOrCreate()
  }

}
