package utils

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach

/**
 * Unit tests for SparkSessionProvider.
 */
class SparkSessionProviderTest extends AnyFunSuite with BeforeAndAfterEach {
  var spark: SparkSession = _

  override def afterEach(): Unit = {
    spark = null
    super.afterEach()
  }

  test("getSession should return a local SparkSession") {
    spark = SparkSessionProvider.getSession("SparkSessionProviderTestSuite")
    assert(spark.isInstanceOf[SparkSession])
    assert(spark.sparkContext.master.startsWith("local"))
  }
} 