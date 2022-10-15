package utils

import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.SparkUtils.getDataFrameFromDB

object SparkSequelUtils {

  /**
   * Transfers tables from a DB to Spark tables
   * A function to transfer all tables to warehouse and create temp views
   * */
  def transferTables(spark: SparkSession, tableNames: List[String], shouldWritAsSparkTable: Boolean): Unit = tableNames.foreach(table => {
    val tableDF = getDataFrameFromDB(spark, table)
    tableDF.createOrReplaceTempView(table)

    if(shouldWritAsSparkTable)
      tableDF.write
        .mode(SaveMode.Overwrite) // Overwrite is not working, supposed to work with path, Not working on Windows
        .saveAsTable(s"rtjvm.$table")
  })

}
