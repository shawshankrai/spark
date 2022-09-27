package learnings.dataframe

import org.apache.spark.sql.types._
import utils.SparkUtils

object DataFrameBasics extends App {

  // Create Spark Session
  val spark = SparkUtils.
    getSparkSession("Dataframe-App-1", "local")

  // Create Dataframe from JSON
  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // Show DF
  carsDF.show()

  // Print Schema
  carsDF.printSchema()

  // First 10
  carsDF.take(10).foreach(println)

  // Spark Types
  val longType = LongType
  val stringType = StringType
  val doubleType = DoubleType

  // Schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  carsDFWithSchema.show()
  carsDFWithSchema.printSchema()

  // create rows by hand
  val carRows = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
  )

  // DFs have schema, rows do not
  val manualCarsDF = spark.createDataFrame(carRows)
  manualCarsDF.show()

  import spark.implicits._
  val manualCarsDFWithImplicits = carRows.toDF("Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs", "Acceleration", "Year", "Origin")
  manualCarsDFWithImplicits.printSchema()
  manualCarsDFWithImplicits.show()

}
