package learnings.datasources

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import utils.SparkUtils

object DataSources extends App {

  // Create Spark Session
  val spark = new SparkUtils().
    getSparkSession("DataSources-App-1", "local")

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

  /* Option - Mode
  * failFast
  * dropMalformed - ignore faulty rows
  * permissive - default
  * */

  /* Option - Path
  * String Path
  * */
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDFWithSchema.show()

  // Options Map
  val carsDFWithSchemaAndOptionMap = spark.read
    .format("json")
    .schema(carsSchema)
    .options(Map(
      "mode" -> "failFast",
      "path" ->"src/main/resources/data/cars.json"
    )).load()

  carsDFWithSchemaAndOptionMap.limit(5).show()

  /* Writing dataframes
  * Format
  * Save Modes - overwrite, append, ignore, errorIfExist
  * Path - Folder Name (cars-write-check.json)
  * Options
  * */
  carsDFWithSchemaAndOptionMap.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars-write-check.json")
  /*
  * Each Partition will have its own file
  * _SUCCESS is a marker file
  * *.crc is for checking data integrity
  * */

  // JSON Flags
  // Schema
  val carsDateSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  * dateFormat will only work with schema
  * Option - allowSingleQuotes
  * Option - compression - bzip2, gzip, snappy, deflate, uncompressed
  * */
  spark.read
    .schema(carsDateSchema)
    .option("allowSingleQuotes", "true")
    .option("dateFormat", "YYYY-MM-dd")
    .option("compression", "uncompressed")
    .json("src/main/resources/data/cars-write-check.json")
    .show()

  // CSV Flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  /*
  * Option - dateFormat
    Option - header
    Option - sep
    Option - nullValue there is no null in CSV if we want to change that
    * */
  spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM d yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")
    .show()

  // Parquet - default storage format
  carsDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars-write-check.parquet")

  // Text Files
  spark.read.text("src/main/resources/data/sample_text.txt").show()

  // JDBC
  val employees = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employees.printSchema()
}
