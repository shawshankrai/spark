package learnings.sequel

import org.apache.spark.sql.DataFrame
import utils.PathGenerators.getPathResourcesMainFolder
import utils.SparkSequelUtils.transferTables
import utils.SparkUtils
import utils.SparkUtils.LOCAL

object SparkSql extends App {

  val spark = SparkUtils.getSparkSession("Spark-Sequel", LOCAL)

  /**
   * Creates a local temporary view using the given name.
   * The lifetime of this temporary view is tied to the SparkSession
   * that was used to create this Dataset
   * Load DF, Create Vies, Use View*/
  val carsDF = spark.read.json(getPathResourcesMainFolder("cars.json"))
  carsDF.createOrReplaceTempView("cars")
  val carsMadeInUSA: DataFrame = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )
  carsMadeInUSA.show()

  /**
   * A db will be created in Folder spark-warehouse
   * default location can be changed by configuration
   * spark.sql.warehouse.dir - src/main/resources/warehouse
   * */
  spark.sql("Create database rtjvm")
  spark.sql("use rtjvm")
  val databaseDF = spark.sql("show databases")
  databaseDF.show()
  /**
  +---------+
  |namespace|
  +---------+
  |  default|
  |    rtjvm|
  +---------+
  **/

  /**
   * Transfers tables from a DB to Spark tables
   * A function to transfer all tables to warehouse and create temp views
   * */
  transferTables(spark, List("titles",
    "movies",
    "employees",
    "departments",
    "dept_manager",
    "dept_emp",
    "salaries")
  , shouldWritAsSparkTable = false)

  // Read spark loaded table
  spark.read.table("employees").show()

}
