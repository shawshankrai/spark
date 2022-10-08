package learnings.typesdatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import utils.SparkUtils

object CommonTypes extends App {
  val spark = SparkUtils.getSparkSession("CommonTypes", "local")
  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Adding a plain value to DF
  moviesDF.select($"Title", lit(47).as("plain_value_column")).show()
  /**
    +--------------------+------------------+
    |               Title|plain_value_column|
    +--------------------+------------------+
    |      The Land Girls|                47|
    |First Love, Last ...|                47|
   **/

  /** Boolean **/
  private val dramaFilter: Column = $"Major_Genre" equalTo "Drama"
  private val goodRating = $"IMDB_Rating" > 7.0
  private val preferredFilter = goodRating and dramaFilter

  moviesDF.select($"Title").where(dramaFilter).show()
  // + multiple ways of filtering

  // filtering on columns
  private val moviesWithGoodnessFlagsDF: DataFrame = moviesDF.select($"Title", preferredFilter.as("good_movie"))
  moviesWithGoodnessFlagsDF.where("good_movie").show() // where($"good_movie" === "true)

  // Negations
  moviesWithGoodnessFlagsDF.where(not($"good_movie")).show()
  /**
  +--------------------+----------+
  |               Title|good_movie|
  +--------------------+----------+
  |      The Land Girls|     false|
  |First Love, Last ...|     false|
  |I Married a Stran...|     false|
  |Let's Talk About Sex|     false|
  |                Slam|     false|
  |             Foolish|     false|
  |             Pirates|     false|
  |     Duel in the Sun|     false|
  |           Tom Jones|     false|
  |             Oliver!|     false|
  |   Hollywood Shuffle|     false|
  |              Wilson|     false|
  |        Darling Lili|     false|
  |The Ten Commandments|     false|
  |                1776|     false|
  |                1941|     false|
  |      Chacun sa nuit|     false|
  |20,000 Leagues Un...|     false|
  |20,000 Leagues Un...|     false|
  |24 7: Twenty Four...|     false|
  +--------------------+----------+
   **/

  /** Numbers **/

  // Math Operators

  // correlations from -1 to 1, low to high
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))
  /** 0.4259708986248317 */

  /** String **/
  val carsDf = spark.read.json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDf.select(initcap($"Name")).show()

  // contains
  carsDf.select("*").where($"Name".contains("audi")).show()

  // regex - extract, replace
  val regexString = "audi"

  // extract
  private val audiDF = carsDf.select(
    $"Name",
    regexp_extract($"Name", regexString, 0).as("regexp_extract") // 0 is for first occurrence
  ).where($"regexp_extract" =!= "")

  audiDF.show()

  // replace
  audiDF.select(
    $"Name",
    regexp_replace($"Name", regexString, "Das Auto").as("regexp_replace")
  ).show()

  /**
  +-------------------+--------------------+
  |               Name|      regexp_replace|
  +-------------------+--------------------+
  |        audi 100 ls|     Das Auto 100 ls|
  |         audi 100ls|      Das Auto 100ls|
  |           audi fox|        Das Auto fox|
  |         audi 100ls|      Das Auto 100ls|
  |          audi 5000|       Das Auto 5000|
  |          audi 4000|       Das Auto 4000|
  |audi 5000s (diesel)|Das Auto 5000s (d...|
  +-------------------+--------------------+
  **/

  // Filter cars df by list provided - Example of or operator in regex
  def getCarNames: List[String] = List("audi", "Ford")
  val carNamesRegex = getCarNames.map(_.toLowerCase()).mkString("|") // audi|ford
  carsDf.select($"Name", regexp_extract($"Name", carNamesRegex, 0).as("pattern"))
    .where($"pattern" =!= "").show()

  /**
  +--------------------+-------+
  |                Name|pattern|
  +--------------------+-------+
  |         ford torino|   ford|
  |    ford galaxie 500|   ford|
  |    ford torino (sw)|   ford|
  |ford mustang boss...|   ford|
  |       ford maverick|   ford|
  |         audi 100 ls|   audi|
  |           ford f250|   ford|
  |          ford pinto|   ford|
  **/

  // with contains - functional programing approach
  val carsNamesFilter = getCarNames.map(_.toLowerCase()).map(name=> $"Name".contains(name))
  val bigFilter = carsNamesFilter.fold(lit(false))((combinedFilter, newCarsNameFilter) => combinedFilter or newCarsNameFilter)
  carsDf.filter(bigFilter).show()

  /** +------------+---------+------------+----------+----------------+--------------------+------+-------------+-------+
  |Acceleration|Cylinders|Displacement|Horsepower|Miles_per_Gallon|                Name|Origin|Weight_in_lbs|      Year|
  +------------+---------+------------+----------+----------------+--------------------+------+-------------+----------+
  |        10.5|        8|       302.0|       140|            17.0|         ford torino|   USA|         3449|1970-01-01|
  |        10.0|        8|       429.0|       198|            15.0|    ford galaxie 500|   USA|         4341|1970-01-01|
  **/
}
