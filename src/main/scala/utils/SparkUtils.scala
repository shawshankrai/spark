package utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  def getSparkSession(appName: String, mode: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config("spark.master", mode)
      .getOrCreate()
  }

  def getDataFrameFromDB(spark: SparkSession, table: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", table)
      .load()
  }
}
