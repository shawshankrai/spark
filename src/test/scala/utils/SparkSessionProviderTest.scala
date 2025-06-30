package utils

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

/**
 * Unit tests for SparkSessionProvider.
 */
class SparkSessionProviderTest extends AnyFunSuite {

  test("getSession should return a SparkSession with the correct app name") {
    val appName = "Test App"
    val spark: SparkSession = SparkSessionProvider.getSession(appName)
    assert(spark.isInstanceOf[SparkSession])
    assert(spark.sparkContext.appName == appName)
    spark.stop()
  }

  test("getSession should return a local SparkSession") {
    val spark: SparkSession = SparkSessionProvider.getSession()
    assert(spark.sparkContext.master.startsWith("local"))
    spark.stop()
  }
} 