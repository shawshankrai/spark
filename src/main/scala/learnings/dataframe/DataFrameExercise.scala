package learnings.dataframe

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import utils.SparkUtils

import java.util

object DataFrameExercise extends App {

  val spark = new SparkUtils().getSparkSession("DataFrameExercise", "local")

  // 1. Create Manual DF
  val phoneSchema = StructType(
    Array(
      StructField("Make", StringType, nullable = true),
      StructField("Model", StringType, nullable = true),
      StructField("Screen_Size", DoubleType, nullable = true),
      StructField("Camera_Dimension", StringType, nullable = true)
    )
  )

  val phoneDataRows = spark.sparkContext.parallelize(Seq(
    Row("SamSung", "G1", 7.0, "40 mpx"),
    Row("Apple", "iPhone", 6.5, "40 mpx"),
    Row("Asus", "Rog", 7.0, "40 mpx"),
    Row("SamSung", "G1", 7.0, "40 mpx")
  ))

  // def createDataFrame(rowRDD: RDD[Row], schema: StructType)
  val phoneDF = spark.createDataFrame(phoneDataRows, phoneSchema)
  phoneDF.printSchema()
  phoneDF.show()

  // 2. Read JSON, Count, Present as DF
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Converting Count From Long to DF
  import spark.implicits._
  val moviesCount = spark.sparkContext.parallelize(Seq(moviesDF.count())).toDF("Count")
  moviesCount.printSchema()
  moviesCount.show()
}
