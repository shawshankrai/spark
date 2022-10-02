package learnings.aggregationsandgrouping

import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, countDistinct, mean, min, stddev, sum}
import utils.SparkUtils

object Aggregations extends App {

  val spark = SparkUtils.getSparkSession("Aggregations", "local")
  import spark.implicits._
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Counting
  val genresCountDFWithSelect = moviesDF.select(count($"Major_Genre")) // select - all the values except nulls
  genresCountDFWithSelect.show()

  val genresCountDFWithSelectExpr = moviesDF.selectExpr("count(Major_genre)") // selectExpr - all the values except nulls
  genresCountDFWithSelectExpr.show()

  // Counting Null
  moviesDF.select(count("*")).show() // select - all the values including nulls

  moviesDF.select(countDistinct($"Major_Genre")).show() // select - Count distinct

  // approximate count
  moviesDF.select(approx_count_distinct($"Major_genre")).show() // select - get approximate  count

  // Min and Max
  moviesDF.select(min($"IMDB_Rating")).show()
  moviesDF.selectExpr("max(IMDB_Rating)").show()

  // Sum
  moviesDF.select(sum($"US_Gross")).show()
  moviesDF.selectExpr("sum(US_gross)").show()

  // Stats - mean and Standard deviation
  moviesDF.select(
    mean($"Rotten_Tomatoes_rating"),
    stddev($"Rotten_Tomatoes_rating")
  ).show()

  // Grouping - Special function count - not as same as earlier count
  val countByGenreDF = moviesDF.groupBy($"Major_Genre").count() // select count(*) from moviesDF group by Major_genre - a;so includes null
  countByGenreDF.show()
  /*
  +-------------------+-----+
  |        Major_Genre|count|
  +-------------------+-----+
  |          Adventure|  274|
  |               null|  275|
  |              Drama|  789|
  |        Documentary|   43|
  |       Black Comedy|   36|
  |  Thriller/Suspense|  239|
  |            Musical|   53|
  |    Romantic Comedy|  137|
  |Concert/Performance|    5|
  |             Horror|  219|
  |            Western|   36|
  |             Comedy|  675|
  |             Action|  420|
  +-------------------+-----+
  */
  
  // Special function Avg
  val averageRating = moviesDF.groupBy($"Major_Genre").avg("IMDB_Rating")

  // Special function agg, sql functions count and avg
  val aggregationByGenreDF = moviesDF.groupBy($"Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy("AvG_rating")
  aggregationByGenreDF.show()
}
