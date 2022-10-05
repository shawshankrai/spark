package learnings.joins

import org.apache.spark.sql.functions.expr
import utils.SparkUtils

object Joins extends App {

  val spark = SparkUtils.getSparkSession("Joins-1", "local")

  val guitarDF = spark.read.json("src/main/resources/data/guitars.json")
  val bandDF = spark.read.json("src/main/resources/data/bands.json")
  val guitaristDF = spark.read.json("src/main/resources/data/guitarPlayers.json")

  /************ Inner Joins ***********/

  /** Join - DF.join(SecondDF, JoinExpression, joinType - inner (default) - all rows from first and second DF, join based on expression) **/
  val bandGuitaristJoinCondition = guitaristDF.col("band") === bandDF.col("id")
  val guitaristBandDF = guitaristDF.join(bandDF, bandGuitaristJoinCondition, "inner")
  guitaristBandDF.show()
  /**
  * +----+-------+---+------------+-----------+---+------------+----+
    |band|guitars| id|        name|   hometown| id|        name|year|
    +----+-------+---+------------+-----------+---+------------+----+
    |   1|    [1]|  1| Angus Young|     Sydney|  1|       AC/DC|1973|
    |   0|    [0]|  0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
    |   3|    [3]|  3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
    +----+-------+---+------------+-----------+---+------------+----+
  **/

  /************ Outer Joins ***********/

  /** Left outer join - leftDF(guitaristDF).join(rightDF(bands, joinExpression, joinType - left))
      Results all rows of inner join + all the rows in left DF with null where data is missing - expression fails
  **/
  guitaristDF.join(bandDF, bandGuitaristJoinCondition, "left").show()
  /**
    +----+-------+---+------------+-----------+----+------------+----+
    |band|guitars| id|        name|   hometown|  id|        name|year|
    +----+-------+---+------------+-----------+----+------------+----+
    |   0|    [0]|  0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
    |   1|    [1]|  1| Angus Young|     Sydney|   1|       AC/DC|1973|
    |   2| [1, 5]|  2|Eric Clapton|       null|null|        null|null|
    |   3|    [3]|  3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
    +----+-------+---+------------+-----------+----+------------+----+
   **/

  /** Right outer join - leftDF(guitaristDF).join(rightDF(bands, joinExpression, joinType - right))
      Results all rows of inner join + all the rows in right DF with null where data is missing - expression fails
  **/
  guitaristDF.join(bandDF, bandGuitaristJoinCondition, "right").show()
  /**
  +----+-------+----+------------+-----------+---+------------+----+
  |band|guitars|  id|        name|   hometown| id|        name|year|
  +----+-------+----+------------+-----------+---+------------+----+
  |   1|    [1]|   1| Angus Young|     Sydney|  1|       AC/DC|1973|
  |   0|    [0]|   0|  Jimmy Page|     London|  0|Led Zeppelin|1968|
  |   3|    [3]|   3|Kirk Hammett|Los Angeles|  3|   Metallica|1981|
  |null|   null|null|        null|  Liverpool|  4| The Beatles|1960|
  +----+-------+----+------------+-----------+---+------------+----+
  **/

  /** Full outer join - leftDF(guitaristDF).join(rightDF(bands, joinExpression, joinType - outer))
      Results all rows of inner join + all the rows from Left Join + all the rows from right Join
   **/
  guitaristDF.join(bandDF, bandGuitaristJoinCondition, "outer").show()
  /**
  +----+-------+----+------------+-----------+----+------------+----+
  |band|guitars|  id|        name|   hometown|  id|        name|year|
  +----+-------+----+------------+-----------+----+------------+----+
  |   0|    [0]|   0|  Jimmy Page|     London|   0|Led Zeppelin|1968|
  |   1|    [1]|   1| Angus Young|     Sydney|   1|       AC/DC|1973|
  |   2| [1, 5]|   2|Eric Clapton|       null|null|        null|null|
  |   3|    [3]|   3|Kirk Hammett|Los Angeles|   3|   Metallica|1981|
  |null|   null|null|        null|  Liverpool|   4| The Beatles|1960|
  +----+-------+----+------------+-----------+----+------------+----+
   * */

  /************ Semi Joins ***********/
  /** All rows from the left DF only - which satisfies the expression */
  guitaristDF.join(bandDF, bandGuitaristJoinCondition, "left_semi").show()
  /**
  +----+-------+---+------------+
  |band|guitars| id|        name|
  +----+-------+---+------------+
  |   0|    [0]|  0|  Jimmy Page|
  |   1|    [1]|  1| Angus Young|
  |   3|    [3]|  3|Kirk Hammett|
  +----+-------+---+------------+
  **/

  /************ Anti Joins ***********/
  /** everything in the left DF which doesn't satisfy the join expression*/
  guitaristDF.join(bandDF, bandGuitaristJoinCondition, "left_anti").show()
  /**
  +----+-------+---+------------+
  |band|guitars| id|        name|
  +----+-------+---+------------+
  |   2| [1, 5]|  2|Eric Clapton|
  +----+-------+---+------------+
  */


  /** Removing Ambiguity
   * 1. Rename ambiguous column and use using column method - will loose the ambiguous column
   * 2. drop duplicate column
   * 3. Rename the column and keep the data - will have same duplicate data
   */

  // this used to crash because of ambiguity in column names
  // guitaristBandDF.select("id", "band").show()
  // Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'id' is ambiguous, could be: id, id.

  // 1 join using column name picks columns with same name
  guitaristDF.join(bandDF.withColumnRenamed("id", "band"), "band").show()
  /**
  +----+-------+---+------------+-----------+------------+----+
  |band|guitars| id|        name|   hometown|        name|year|
  +----+-------+---+------------+-----------+------------+----+
  |   1|    [1]|  1| Angus Young|     Sydney|       AC/DC|1973|
  |   0|    [0]|  0|  Jimmy Page|     London|Led Zeppelin|1968|
  |   3|    [3]|  3|Kirk Hammett|Los Angeles|   Metallica|1981|
  +----+-------+---+------------+-----------+------------+----+
  */

  // 2
  guitaristDF.drop(bandDF.col("id")).show()
  /**
  +----+-------+---+------------+
  |band|guitars| id|        name|
  +----+-------+---+------------+
  |   0|    [0]|  0|  Jimmy Page|
  |   1|    [1]|  1| Angus Young|
  |   2| [1, 5]|  2|Eric Clapton|
  |   3|    [3]|  3|Kirk Hammett|
  +----+-------+---+------------+
  **/

  // 3 withColumnRenamed("existing", "new")
  val bandsModDF = bandDF.withColumnRenamed("id", "bandsId")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandsId")).show()

  /**
  +----+-------+---+------------+-----------+-------+------------+----+
  |band|guitars| id|        name|   hometown|bandsId|        name|year|
  +----+-------+---+------------+-----------+-------+------------+----+
  |   1|    [1]|  1| Angus Young|     Sydney|      1|       AC/DC|1973|
  |   0|    [0]|  0|  Jimmy Page|     London|      0|Led Zeppelin|1968|
  |   3|    [3]|  3|Kirk Hammett|Los Angeles|      3|   Metallica|1981|
  +----+-------+---+------------+-----------+-------+------------+----+
  **/

  /********** joining complex structures ********/
  // Can use any kind of expression as a join expression
  guitaristDF
    .join(guitarDF.withColumnRenamed("id", "guitarID"), expr("array_contains(guitars, guitarID)")).show()
  /**
  +----+-------+---+------------+--------------------+--------+------+------------+
  |band|guitars| id|        name|          guitarType|guitarID|  make|       model|
  +----+-------+---+------------+--------------------+--------+------+------------+
  |   0|    [0]|  0|  Jimmy Page|Electric double-n...|       0|Gibson|    EDS-1275|
  |   2| [1, 5]|  2|Eric Clapton|            Electric|       5|Fender|Stratocaster|
  |   1|    [1]|  1| Angus Young|            Electric|       1|Gibson|          SG|
  |   2| [1, 5]|  2|Eric Clapton|            Electric|       1|Gibson|          SG|
  |   3|    [3]|  3|Kirk Hammett|            Electric|       3|   ESP|        M-II|
  +----+-------+---+------------+--------------------+--------+------+------------+
  **/

}
