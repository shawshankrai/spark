package learnings.lowlevelspark

import org.apache.spark.rdd.RDD
import utils.SparkUtils
import utils.SparkUtils.LOCAL

import scala.io.Source

object Rdd extends App {

  val spark = SparkUtils.getSparkSession("Rdds", LOCAL)

  /**
   * RDDs are same as datasets, both are distributed scala objects
   * DataFrame == Dataset[Row]
   * rdd have minute configuration. cache, storageLevel, persist
   * Dataset have a hand over rdd: Select and Joins
   **/
  // Ways of creating RDD
  val numbers = spark.sparkContext.parallelize(1 to 100000)

  // reading from file
  case class StockValue(company: String, date: String, price: Double)
  def readFile(fileName: String): List[StockValue] =
    Source.fromFile(fileName).getLines().drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  // From Source
  val stocksRDD: RDD[StockValue] = spark.sparkContext.parallelize(readFile("src/main/resources/data/stocks.csv"))

  // From Spark context
  val stockRDD2: RDD[StockValue] = spark.sparkContext.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // From DS and DF
  val stocksDF = spark.read.csv("src/main/resources/data/stocks.csv")
  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd
  val stocksRDD4 = stocksDF.rdd

  // From RDD -> DF
  val numbersDF = numbers.toDF("numbers") // Parameter is column name, will loose type information

  // rdd -> DS
  val numberDS = spark.createDataset(numbers)// will have type information

}
