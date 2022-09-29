package learnings.dataframecolumns

import utils.SparkUtils

object DataframeColumnExercise extends App {

  /**
   * Read Movies DF
   * Sum Up US + World_wide + DVD
   * Select All Good Comedies = Rotten > 33, IMDB > 6
   *
   * Use Multiple Versions
  **/

  val spark = SparkUtils.getSparkSession("DF_Column_Exercise-1", "local")
  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  val totalRevenue = moviesDF
    .na.fill(0, Seq("US_DVD_Sales", "US_Gross", "Worldwide_Gross"))
    .selectExpr("Title",
      "US_DVD_Sales",
      "US_Gross",
      "Worldwide_Gross",
      "(US_DVD_Sales + US_Gross + Worldwide_Gross) as total_revenue")
  totalRevenue.show()

  val goodComedies = moviesDF
    .na.fill(Map("Rotten_Tomatoes_Rating" -> 0, "IMDB_Rating"-> 0.0))
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6.0 and Rotten_Tomatoes_Rating > 33")
    .select($"Title", $"Major_Genre", $"IMDB_Rating", $"Rotten_Tomatoes_Rating")
  goodComedies.show()
}
