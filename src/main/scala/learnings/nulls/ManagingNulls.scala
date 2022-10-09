package learnings.nulls

import org.apache.spark.sql.functions.coalesce
import utils.SparkUtils

object ManagingNulls extends App {

  val spark = SparkUtils.getSparkSession("ComplexTypes", "local", "ERROR")
  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // select the first npn-null value
  moviesDF.select(
    $"Title",
    $"Rotten_Tomatoes_Rating",
    $"IMDB_Rating",
    coalesce($"Rotten_Tomatoes_Rating", $"IMDB_Rating" * 10).as("Rating") // picks non-null out of these two
  ).show()

  /**
  +--------------------+----------------------+-----------+------+
  |               Title|Rotten_Tomatoes_Rating|IMDB_Rating|Rating|
  +--------------------+----------------------+-----------+------+
  |      The Land Girls|                  null|        6.1|  61.0|
  |First Love, Last ...|                  null|        6.9|  69.0|
  |I Married a Stran...|                  null|        6.8|  68.0|
  |Let's Talk About Sex|                    13|       null|  13.0|
  **/

  // Checking for null
  moviesDF.select("*").where($"Rotten_Tomatoes_Rating".isNull).show()

  // nulls when ordering
  moviesDF.orderBy($"IMDB_Rating".desc_nulls_last).show()

  // removing null
  moviesDF.select("Title", "IMDB_Rating").na.drop().show() // remove column containing null

  // replace nulls
  moviesDF.na.fill(0.0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show() // List - accurately saves with correct type double-int
  moviesDF.na.fill(Map("IMDB_Rating"-> 0.0, "Rotten_Tomatoes_Rating" -> 0, "Director" -> "Unknown")).show() // Map

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(IMDB_Rating, Rotten_Tomatoes_Rating * 10) as ifnull", // coalesce
    "nvl(IMDB_Rating, Rotten_Tomatoes_Rating * 10) as nvl", // Same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif" , // two are equal then null, else first
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if first is not null then second else third
  ).show()
  /**
  +--------------------+-----------+----------------------+------+-----+------+----+
  |               Title|IMDB_Rating|Rotten_Tomatoes_Rating|ifnull|  nvl|nullif|nvl2|
  +--------------------+-----------+----------------------+------+-----+------+----+
  |      The Land Girls|        6.1|                  null|   6.1|  6.1|  null| 0.0|
  |First Love, Last ...|        6.9|                  null|   6.9|  6.9|  null| 0.0|
  |I Married a Stran...|        6.8|                  null|   6.8|  6.8|  null| 0.0|
  **/


}
