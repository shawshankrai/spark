package learnings.dataframecolumns

import org.apache.spark.sql.functions.{col, column, expr}
import utils.SparkUtils

object DataFrameColumn extends App {

  val spark = SparkUtils.getSparkSession("Columns-1", "local")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // Projection
  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // Interpolated string, returns a column object
    expr("Origin") // Expression
  ).show()

  // Select with plane column names
  carsDF.select("Name", "Year").show()

  // Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightsInKgs = carsDF.col("Weight_in_lbs") / 3.1

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    simplestExpression.as("Weight_lbs"),
    weightsInKgs.as("Weight_kgs"))

  carsWithWeightDF.show()

  // Expr
  val carsWithExpr = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 3.3"
  )

  // DF Processing
  // add new Column
  val carsWithKGDF = carsDF.withColumn("Weight_in_kgs", $"weight_in_lbs" / 3.3)

  // Renaming
  val carsColumnRenamedDF = carsWithKGDF.withColumnRenamed("Weight_in_kgs", "Weight_Kilograms")
  carsColumnRenamedDF.show()

  //Remove
  val carsColDropped = carsColumnRenamedDF.drop("Weight_Kilograms")
  carsColDropped.printSchema()

  // Filtering
  val nonUSACarsFilterDF = carsDF.filter(col("Origin") =!= "USA")
  val nonUSACarsWhereDF = carsDF.where(col("Origin") =!= "USA")
  val USACarsWhereDF = carsDF.filter("Origin = 'USA'")
  // and is an infix operator
  val USACarsWherePowerfulDF = carsDF.filter($"Origin" === "USA" and $"Horsepower" > 190)
  USACarsWherePowerfulDF.show()
  // With expression
  val USACarsWherePowerfulDFExpr = carsDF.filter("Origin ='USA' and Horsepower > 190")
  USACarsWherePowerfulDFExpr.show()

  // Union
  val moreCarsDF = spark.read.option("inferSchema", value = true).json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  // Distinct
  val distinctOriginDF = allCarsDF.select("Origin").distinct()
  distinctOriginDF.show()
}