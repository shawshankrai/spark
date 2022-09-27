package learnings.datasources

import org.apache.spark.sql.types._
import utils.SparkUtils

object DataSourceExercise extends App {

  val spark = SparkUtils.getSparkSession("DataSources-Exercise-1", "local")

  var moviesSchema = StructType(
    Array(
      StructField("Title", StringType, nullable = true),
      StructField("US_Gross", IntegerType, nullable = true),
      StructField("Worldwide_Gross", IntegerType, nullable = true),
      StructField("Creative_Type", StringType, nullable = true),
      StructField("Director", StringType, nullable = true),
      StructField("Distributor", StringType, nullable = true),
      StructField("IMDB_Rating", StringType, nullable = true),
      StructField("IMDB_Votes", StringType, nullable = true),
      StructField("MPAA_Rating", StringType, nullable = true),
      StructField("Major_Genre", StringType, nullable = true),
      StructField("Production_Budget", LongType, nullable = true),
      StructField("Release_Date", StringType, nullable = true),
      StructField("Rotten_Tomatoes_Rating", LongType, nullable = true),
      StructField("Running_Time_min", LongType, nullable = true),
      StructField("Source", StringType, nullable = true),
      StructField("US_DVD_Sales", LongType, nullable = true)
    )
  )

  val moviesDF = spark.read
    .schema(moviesSchema)
    .json("src/main/resources/data/movies.json")

  //
  moviesDF.write.option("sep", "\t").csv("src/main/resources/data/movies.csv")

  // Write in Parquet
  moviesDF.write.parquet("src/main/resources/data/movies.parquet")

  // Write in Postgres
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}
