package learnings.lowlevelspark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import utils.PathGenerators.getPathResourcesMainFolderWithFile
import utils.SparkUtils
import utils.SparkUtils.LOCAL

import scala.io.Source

object Rdd extends App {

  val spark = SparkUtils.getSparkSession("Rdds", LOCAL)
  import spark.implicits._

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
  def readFile(fileName: String): List[StockValue] = {
    val source = Source.fromFile(fileName)

    val list = source.getLines().drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

    source.close()
    list
  }

  // From Source
  val stocksRDD: RDD[StockValue] = spark.sparkContext.parallelize(readFile("src/main/resources/data/stocks.csv"))
  stocksRDD.take(5).foreach(println(_))

  /**
   * StockValue(MSFT,Jan 1 2000,39.81)
   * StockValue(MSFT,Feb 1 2000,36.35)
   * StockValue(MSFT,Mar 1 2000,43.22)
   * StockValue(MSFT,Apr 1 2000,28.37)
   * StockValue(MSFT,May 1 2000,25.45)
   */

  // From Spark context
  val stockRDD2: RDD[StockValue] = spark.sparkContext.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
  stockRDD2.take(5).foreach(println)
  /**
   * StockValue(MSFT,Jan 1 2000,39.81)
   * StockValue(MSFT,Feb 1 2000,36.35)
   * StockValue(MSFT,Mar 1 2000,43.22)
   * StockValue(MSFT,Apr 1 2000,28.37)
   * StockValue(MSFT,May 1 2000,25.45)
   * */

  // From DS and DF
  val stocksDF = spark.read.options(Map(
    "header" -> "true",
    "inferSchema" -> "true"
  )).csv("src/main/resources/data/stocks.csv")

  val stocksDS = stocksDF.as[StockValue]

  val stocksRDD3 = stocksDS.rdd
  stocksRDD3.take(5).foreach(println)
  /**
   * [MSFT,Jan 1 2000,39.81]
   * [MSFT,Feb 1 2000,36.35]
   * [MSFT,Mar 1 2000,43.22]
   * [MSFT,Apr 1 2000,28.37]
   * [MSFT,May 1 2000,25.45]
   *
   * */

  val stocksRDD4 = stocksDF.rdd
  stocksRDD4.take(5).foreach(println(_))
  /**
   * [MSFT,Jan 1 2000,39.81]
   * [MSFT,Feb 1 2000,36.35]
   * [MSFT,Mar 1 2000,43.22]
   * [MSFT,Apr 1 2000,28.37]
   * [MSFT,May 1 2000,25.45]
   *
   * */

  // From RDD -> DF
  val numbersDF = numbers.toDF("numbers") // Parameter is column name, will loose type information
  numbersDF.show()
  /**
   * +-------+
   * |numbers|
   * +-------+
   * |      1|
   * |      2|
   * |      3|
   * */

  // rdd -> DS
  val numberDS = spark.createDataset(numbers) // will have type information
  numberDS.show()
  /**
   * +-------+
   * |numbers|
   * +-------+
   * |      1|
   * |      2|
   * |      3|
   * */

  // Transformation
  private val microsoftRDD: RDD[StockValue] = stocksRDD.filter(_.company == "MSFT") // Transformation - lazy
  println(s"Microsoft Share Count: ${microsoftRDD.count()}") // eager Action

  private val distinctStocks: RDD[String] = stocksRDD.map(_.company).distinct() // lazy transformation
  println(s"Distinct Stock Count: ${distinctStocks.count()}")

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((currentStock, otherStock) => currentStock.price < otherStock.price)
  println(s"Minimum Stock Price: ${microsoftRDD.min()}") // Action

  // reduce
  numbers.reduce(_ + _)

  // grouping
  private val groupedStocks: RDD[(String, Iterable[StockValue])] = stocksRDD.groupBy(_.company) // Very expensive - Wide transformation
  groupedStocks.take(10).foreach(println(_))

  // partitioning
  private val partitionedRDD: RDD[StockValue] = stocksRDD.repartition(3)
  partitionedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet(getPathResourcesMainFolderWithFile("stocksDF")) // will create 3 files, Very expensive - Wide transformation

  // coalesce, default - shuffling off, will mostly provide less partition then mentioned
  stocksRDD.coalesce(15).toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet(getPathResourcesMainFolderWithFile("coalesceStocks"))
}
