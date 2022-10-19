package bigdata

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.hour
import utils.PathGenerators.getPathResourcesMainFolderWithFile
import utils.SparkUtils
import utils.SparkUtils.LOCAL

object TaxiApplication extends App {

  val spark = SparkUtils.getSparkSession("Big_Data_Application", LOCAL)
  import spark.implicits._

  val bigTaxiDF  = spark.read.parquet("C:\\Users\\shash\\Downloads\\NYC_taxi_2009-2016.parquet")
  //val taxiDF = bigTaxiDF
  val taxiDF = spark.read.parquet(getPathResourcesMainFolderWithFile("yellow_taxi_jan_25_2018"))
  taxiDF.printSchema()
  taxiDF.show()
  taxiDF.select(functions.count($"*")).show()

  val taxiZoneDF = spark.read.option("header", "true").csv(getPathResourcesMainFolderWithFile("taxi_zones.csv"))
  taxiZoneDF.printSchema()
  taxiZoneDF.show()

  /**
   * Questions:
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   * 2. What are the peak hours for taxi?
   * 3. How are the trips distributed by length? Why are people taking the cab?
   * 4. What are the peak hours for long/short trips?
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   * 6. How are people paying for the ride, on long/short trips?
   * 7. How is the payment type evolving with time?
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   */

  // 1 - Most visited
  val pickupsByTaxiDF = taxiDF.groupBy($"PULocationID" as "LocationID")
    .agg(functions.count($"*") as "totalTrips")

  val dropOffsByTaxiDF = taxiDF.groupBy($"DOLocationID" as "LocationID")
    .agg(functions.count($"*") as "totalTrips")

  val totalTripsDF = pickupsByTaxiDF
     //.union(dropOffsByTaxiDF)  // uncomment if want to add drop offs
    .groupBy($"LocationID")
    .agg(functions.sum($"totalTrips") as "totalTrips")

  val totalTripsByZoneDF = taxiZoneDF.join(totalTripsDF, Seq("LocationID"), "left").orderBy($"totalTrips".desc)

  /**
   * +----------+---------+--------------------+------------+----------+
   * |LocationID|  Borough|                Zone|service_zone|totalTrips|
   * +----------+---------+--------------------+------------+----------+
   * |       237|Manhattan|Upper East Side S...| Yellow Zone|     15945|
   * |       161|Manhattan|      Midtown Center| Yellow Zone|     15255|
   * |       236|Manhattan|Upper East Side N...| Yellow Zone|     13767|
   * |       162|Manhattan|        Midtown East| Yellow Zone|     13715|
   * */

  // 1 - b
  val pickupByBorough = totalTripsByZoneDF
    .groupBy("Borough")
    .agg(functions.sum("totalTrips") as "totalTrips")
    .orderBy($"totalTrips".desc)
  /**
   * +-------------+----------+
   * |      Borough|totalTrips|
   * +-------------+----------+
   * |    Manhattan|    304266|
   * |       Queens|     17712|
   * |      Unknown|      6644|
   * |     Brooklyn|      3037|
   * |        Bronx|       211|
   * |          EWR|        19|
   * |Staten Island|         4|
   * +-------------+----------+
   * */

  // 2 - peek hours
  val pickUpByHour = taxiDF.withColumn("hour_of_day", hour($"tpep_pickup_datetime"))
    .groupBy("hour_of_day")
    .agg(functions.count("*") as "totalTrips")
    .orderBy($"totalTrips".desc)
  /**
   * +-----------+----------+
   * |hour_of_day|totalTrips|
   * +-----------+----------+
   * |         22|     22108|
   * |         21|     20924|
   * |         23|     20903|
   * |          1|     20831|
   * |          0|     20421|
   * |         18|     18316|
   * |         11|     18270|
   * |         12|     17983|
   * |          2|     17097|
   * |         19|     16862|
   * |         17|     16741|
   * |         20|     16638|
   * |         15|     16194|
   * |         13|     15988|
   * |         16|     15613|
   * |         14|     15162|
   * |         10|     11964|
   * |          3|     10856|
   * |          9|      5358|
   * |          4|      5127|
   * +-----------+----------+
   * */


}
