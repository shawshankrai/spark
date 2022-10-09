package learnings.types

import org.apache.spark.sql.functions.{array_contains, expr, size, split, struct, to_date}
import utils.SparkUtils

object ComplexTypes extends App {

  val spark = SparkUtils.getSparkSession("ComplexTypes", "local", "ERROR")
  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Dates - Conversion after DF is generated, date format changed after Spark 3
  moviesDF.select($"Title", to_date($"Release_Date", "d-MMM-yy").as("Actual_Release")).show()

  // Structures
  // With Select
  moviesDF.select($"Title", struct($"US_GROSS", $"Worldwide_Gross").as("Profit"))
    .select($"Title", $"Profit".getField("US_Gross").as("US_Profit")).show()

  // With SelectExpr
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_gross").show()

  // Arrays
  val moviesWithWords = moviesDF.select($"Title", split($"Title", "|,").as("Title_Words"))
  moviesWithWords.select(
    $"Title",
    expr("Title_Words[0]"),
    size($"Title_Words"),
    array_contains($"Title_Words", "love")
  ).show()

  /**
  +--------------------+--------------+-----------------+---------------------------------+
  |               Title|Title_Words[0]|size(Title_Words)|array_contains(Title_Words, love)|
  +--------------------+--------------+-----------------+---------------------------------+
  |      The Land Girls|             T|               15|                            false|
  |First Love, Last ...|             F|               23|                            false|
  |I Married a Stran...|             I|               27|                            false|
  |Let's Talk About Sex|             L|               21|                            false|
  **/
}
