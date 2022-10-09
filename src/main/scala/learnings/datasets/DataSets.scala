package learnings.datasets

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types._
import utils.PathGenerators.getPathResourcesMainFolder
import utils.SparkUtils

import java.sql.Date

/** Performance is critical: In datasets Spark cant optimize transformation
 * the big but
 * type DataFrame = Dataset[Row]
 * need to find out more
 * */
object DataSets extends App {
  val spark = SparkUtils.getSparkSession("ComplexTypes", "local", "ERROR")
  import spark.implicits._

  val numbersDF = spark.read
    .option("header", value = true)
    .schema("""numbers long""")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  numbersDF.show()

  /**
   * Schema
   * root
        |-- numbers: string (nullable = true) ----- this needs to be resolved

    +-------+
    |numbers|
    +-------+
    | 953836|
    | 619973|

  **/

  /**
    Dataset - JVM The Dataframes provide API quickly to perform aggregation operations.
    The RDDs are slower than both the Dataframes and the Datasets to perform simple functions like
    data grouping. The Dataset is faster than the RDDs but is a bit slower than Dataframes.
    Hence, it performs aggregation faster than RDD and the Dataset

      But -----> type DataFrame = Dataset[Row] how can it be faster, is it only for representation
      because datasets and dataframes are different
  */
  // Encoder -- removed, all encoders are present in implicit
  val numberDS: Dataset[Long] = numbersDF.as[Long]
  numberDS.filter(_.>(50)).show()
  /**
  +-------+
  |numbers|
  +-------+
  | 953836|
  | 619973|
  | 961724|
  **/

  /****** Creating custom DS ******/

  // 1 - Create Case Class, had to add Option, Dataframe null was throwing error in Dataset
  case class Car( Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Option[Long],
                  Displacement: Option[Double],
                  Horsepower: Option[Long],
                  Weight_in_lbs: Option[Long],
                  Acceleration: Option[Double],
                  Year: Option[Date],
                  Origin: Option[String]
                )

  // 2 - Read Df
  val carsDateSchema = StructType(Array(
    StructField("Name", StringType, nullable =  true),
    StructField("Miles_per_Gallon", DoubleType, nullable =  true),
    StructField("Cylinders", LongType, nullable =  true),
    StructField("Displacement", DoubleType, nullable =  true),
    StructField("Horsepower", LongType, nullable =  true),
    StructField("Weight_in_lbs", LongType, nullable =  true),
    StructField("Acceleration", DoubleType, nullable =  true),
    StructField("Year", DateType, nullable =  true),
    StructField("Origin", StringType, nullable =  true)
  ))
  val carsDF: DataFrame = spark.read.schema(carsDateSchema) // added schema because of type casting error
    .option("dateFormat", "YYYY-MM-dd")
    .json(getPathResourcesMainFolder("cars.json"))

  carsDF.printSchema()

  // 3 - Convert to DF, using encoder present in implicits
  val carsDS = carsDF.as[Car]
  carsDS.filter(_.Name.toUpperCase.contains("FORD")).show()

  /**
  +--------------------+----------------+---------+------------+----------+-------------+------------+----------+------+
  |                Name|Miles_per_Gallon|Cylinders|Displacement|Horsepower|Weight_in_lbs|Acceleration|      Year|Origin|
  +--------------------+----------------+---------+------------+----------+-------------+------------+----------+------+
  |         ford torino|            17.0|        8|       302.0|       140|         3449|        10.5|1970-01-01|   USA|
  |    ford galaxie 500|            15.0|        8|       429.0|       198|         4341|        10.0|1970-01-01|   USA|
  |    ford torino (sw)|            null|        8|       351.0|       153|         4034|        11.0|1970-01-01|   USA|
  |ford mustang boss...|            null|        8|       302.0|       140|         3353|         8.0|1970-01-01|   USA|
  **/


  /**
   * count
   * count with filter HP > 140
   * AVG HP
   * */

  println(carsDS.count())
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count())
  carsDS.select(avg($"Horsepower")).show()

  /**
   * Difference in results may be because of nulls
    406
    81
    103
    +---------------+
    |avg(Horsepower)|
    +---------------+
    |       105.0825|
    +---------------+
   * */
}
