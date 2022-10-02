package learnings.aggregationsandgrouping

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, countDistinct, mean, stddev, sum}
import utils.SparkUtils

object AggregationsExercise extends App {

  val spark = SparkUtils.getSparkSession("AggregationsExercise", "local")
  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
    .na.fill(0, Seq("US_DVD_Sales", "US_Gross", "Worldwide_Gross"))
  moviesDF.show()

  // Sum up all profits
  moviesDF.selectExpr("sum(US_DVD_Sales + US_Gross + Worldwide_Gross)").show()

  // Count distinct directors
  moviesDF.select(countDistinct($"Director")).show()

  // Mean and Std dev for revenue
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // Avg imdb rating und us gross
  moviesDF.na.fill(0.0, Seq("IMDB_Rating"))
    .groupBy($"Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    ).orderBy($"Avg_IMDB_Rating".desc, $"Total_US_Gross".desc)
    .show()

  /*
  +-----------------+-----------------+--------------+
  |         Director|  Avg_IMDB_Rating|Total_US_Gross|
  +-----------------+-----------------+--------------+
  |       Katia Lund|              8.8|       7563397|
  |      Pete Docter|              8.4|     293004164|
  |     John Sturges|              8.4|      11744471|
  |    Stanley Donen|              8.4|       3600000|
  |   Andrew Stanton|             8.35|     563523142|
  |   Neill Blomkamp|              8.3|     115646235|
  |  Catherine Owens|              8.3|      10363341|
  |Quentin Tarantino|             8.25|     407571061|
  |     Milos Forman|8.233333333333334|     195534939|
  |Christopher Nolan|              8.2|    1170262057|
  |    William Wyler|              8.2|      96600000|
  |   Akira Kurosawa|8.100000000000001|        320592|
  |      Frank Capra|             8.08|      27100000|
  |        Brad Bird|8.033333333333333|     491046051|
  |     Edgar Wright|8.033333333333333|      68329055|
  |     Billy Wilder|            8.025|      69600000|
  |    Peter Jackson|8.016666666666667|    1295938758|
  |      Joss Whedon|              8.0|      25514517|
  |   Richard Brooks|7.966666666666668|      40970324|
  |       Elia Kazan|7.966666666666666|      25400000|
  +-----------------+-----------------+--------------+
  */
}
