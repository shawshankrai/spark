package bigdata

import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{hour, mean, not, stddev}
import utils.PathGenerators.getPathResourcesMainFolderWithFile
import utils.SparkUtils
import utils.SparkUtils.LOCAL

object TaxiApplication extends App {

  val spark = SparkUtils.getSparkSession("Big_Data_Application", LOCAL)
  import spark.implicits._

  //val bigTaxiDF  = spark.read.parquet("C:\\Users\\shash\\Downloads\\NYC_taxi_2009-2016.parquet")
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

  // 3
  val tripDistanceDF = taxiDF.select($"trip_distance".as("distance"))
  val longDistanceThreshold = 30
  val tipsDistanceStatsDF = tripDistanceDF.select(
    functions.count("*") as "count",
    mean("distance") as "mean",
    stddev("distance") as "stddev",
    functions.min("distance") as "min",
    functions.max("distance") as "max"
  )

  /**
   * +------+-----------------+-----------------+---+----+
   * | count|             mean|           stddev|min| max|
   * +------+-----------------+-----------------+---+----+
   * |331893|2.717989442380494|3.485152224885052|0.0|66.0|
   * +------+-----------------+-----------------+---+----+
   * */

  val tripsWithLengthDF = taxiDF.withColumn("isLong", $"trip_distance" >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()

  /**
   * +------+------+
   * |isLong| count|
   * +------+------+
   * |  true|    83|
   * | false|331810|
   * +------+------+
   * */

  // 4
  val pickUpByHourByLengthDF = tripsWithLengthDF.withColumn("hour_of_day", hour($"tpep_pickup_datetime"))
    .groupBy("hour_of_day", "isLong")
    .agg(functions.count("*") as "totalTrips")
    .orderBy($"totalTrips".desc)
  pickUpByHourByLengthDF.show(48)

  /**
   * +-----------+------+----------+
   * |hour_of_day|isLong|totalTrips|
   * +-----------+------+----------+
   * |         22| false|     22104|
   * |         21| false|     20921|
   * |         23| false|     20897|
   * |          1| false|     20824|
   * |          0| false|     20412|
   * |         18| false|     18315|
   * |         11| false|     18264|
   * |         12| false|     17980|
   * |          2| false|     17094|
   * |         19| false|     16858|
   * |         17| false|     16734|
   * |         20| false|     16635|
   * |         15| false|     16190|
   * |         13| false|     15985|
   * |         16| false|     15610|
   * |         14| false|     15158|
   * |         10| false|     11961|
   * |          3| false|     10853|
   * |          9| false|      5358|
   * |          4| false|      5124|
   * |          5| false|      3194|
   * |          6| false|      1971|
   * |          8| false|      1803|
   * |          7| false|      1565|
   * |          0|  true|         9|
   * |          1|  true|         7|
   * |         17|  true|         7|
   * |         11|  true|         6|
   * |         23|  true|         6|
   * |         19|  true|         4|
   * |         14|  true|         4|
   * |         22|  true|         4|
   * |         15|  true|         4|
   * |         10|  true|         3|
   * |          2|  true|         3|
   * |         20|  true|         3|
   * |          3|  true|         3|
   * |         16|  true|         3|
   * |          4|  true|         3|
   * |         12|  true|         3|
   * |         21|  true|         3|
   * |         13|  true|         3|
   * |          6|  true|         2|
   * |          5|  true|         2|
   * |         18|  true|         1|
   * +-----------+------+----------+
   * */

  // 5
  def pickupDropOffPopularity(predicate: Column) = tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID").agg(functions.count("*") as "totalTrips") // count by tuple pickup - dropOff
    .join(taxiZoneDF, $"PULocationID" === $"LocationID")   // join based on pickup
    .withColumnRenamed("Zone", "Pickup_zone")  // get pickup zone details
    .drop("LocationID", "Borough", "service_zone")   // remove other column
    .join(taxiZoneDF, $"DOLocationID" === $"LocationID")   // join based on drop off
    .withColumnRenamed("Zone", "DropOff_zone")  // get pickup zone details
    .drop("LocationID", "Borough", "service_zone")  // remove other column
    .drop("PULocationID", "DOLocationID")  // remove IDs
    .orderBy($"totalTrips".desc)

  pickupDropOffPopularity($"isLong").show()

  /**
   * +----------+--------------------+--------------------+
   * |totalTrips|         Pickup_zone|        DropOff_zone|
   * +----------+--------------------+--------------------+
   * |        14|         JFK Airport|                  NA|
   * |         8|   LaGuardia Airport|                  NA|
   * |         4|         JFK Airport|         JFK Airport|
   * |         4|         JFK Airport|      Newark Airport|
   * |         3|                  NV|                  NV|
   * |         3|       Midtown South|                  NA|
   * |         2|       Midtown North|      Newark Airport|
   * |         2|         JFK Airport|Riverdale/North R...|
   * |         2|Penn Station/Madi...|                  NA|
   * |         2|        Clinton East|                  NA|
   * |         2|   LaGuardia Airport|      Newark Airport|
   * |         1|Financial Distric...|         JFK Airport|
   * |         1|         JFK Airport|         Fort Greene|
   * |         1|         JFK Airport|Van Nest/Morris Park|
   * |         1| Little Italy/NoLiTa|Charleston/Totten...|
   * |         1|         JFK Airport|Eltingville/Annad...|
   * |         1|         JFK Airport|       Arden Heights|
   * |         1|         JFK Airport|Prospect-Lefferts...|
   * |         1|      Midtown Center|Charleston/Totten...|
   * |         1|            Flushing|                  NA|
   * +----------+--------------------+--------------------+
   *
   * */

  pickupDropOffPopularity(not($"isLong")).show()
  /**
   * +----------+--------------------+--------------------+
   * |totalTrips|         Pickup_zone|        DropOff_zone|
   * +----------+--------------------+--------------------+
   * |      5558|                  NV|                  NV|
   * |      2425|Upper East Side S...|Upper East Side N...|
   * |      1962|Upper East Side N...|Upper East Side S...|
   * |      1944|Upper East Side N...|Upper East Side N...|
   * |      1928|Upper East Side S...|Upper East Side S...|
   * |      1052|Upper East Side S...|      Midtown Center|
   * |      1012|Upper East Side S...|        Midtown East|
   * |       987|      Midtown Center|Upper East Side S...|
   * |       965|Upper West Side S...|Upper West Side N...|
   * |       882|      Midtown Center|      Midtown Center|
   * |       865|     Lenox Hill West|Upper East Side N...|
   * |       850|Penn Station/Madi...|      Midtown Center|
   * |       828|Upper West Side N...|Upper West Side S...|
   * |       824|Upper West Side S...| Lincoln Square East|
   * |       809| Lincoln Square East|Upper West Side S...|
   * |       808|     Lenox Hill West|Upper East Side S...|
   * |       797|        Midtown East|         Murray Hill|
   * |       784|Upper East Side S...|     Lenox Hill West|
   * |       763|      Yorkville West|Upper East Side N...|
   * |       757|Times Sq/Theatre ...|Penn Station/Madi...|
   * +----------+--------------------+--------------------+
   * */
}
