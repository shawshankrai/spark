package learnings.lowlevelspark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{avg, col, not}
import utils.{PathGenerators, SparkUtils}
import utils.SparkUtils.LOCAL

import scala.io.Source

object RddExercise extends App {

  val spark = SparkUtils.getSparkSession("RDD-Exercise", LOCAL)

  /**
   * read movies as RDD
   * Show distinct genres as an RDD
   * Select all the drama movies with rating > 6
   * Show avg rating of movies by genre
   * */

  case class Movie(Title: String, Major_Genre: String, IMDB_Rating: Double)

  // 1
  import spark.implicits._

  // Dropping nulls
  spark.read
    .json(PathGenerators.getPathResourcesMainFolderWithFile("movies.json"))
    .na.drop(Seq("IMDB_Rating", "Title", "Major_Genre"))
    .show()

  private val moviesRDD: RDD[Movie] = spark.read
    .json(PathGenerators.getPathResourcesMainFolderWithFile("movies.json"))
    .filter($"Major_Genre".isNotNull and $"Title".isNotNull and $"IMDB_Rating".isNotNull)
    .as[Movie].rdd
  moviesRDD.take(10).foreach(println(_))
  /**
   * Movie(The Land Girls,null,Some(6.1))
   * Movie(First Love, Last Rites,Drama,Some(6.9))
   * Movie(I Married a Strange Person,Comedy,Some(6.8))
   * Movie(Let's Talk About Sex,Comedy,None)
   * Movie(Slam,Drama,Some(3.4))
   * Movie(Mississippi Mermaid,null,None)
   * Movie(Following,null,Some(7.7))
   * Movie(Foolish,Comedy,Some(3.8))
   * Movie(Pirates,null,Some(5.8))
   * Movie(Duel in the Sun,null,Some(7.0))
   * */

  // 2
  private val distinctMoviesGenre: RDD[String] = moviesRDD.map(_.Major_Genre).distinct()
  distinctMoviesGenre.take(10).foreach(println(_))
  /**
   * Concert/Performance
   * Western
   * Musical
   * Horror
   * Romantic Comedy
   * Comedy
   * Black Comedy
   * Documentary
   * Adventure
   * */

  // 3
  private val goodDrama: RDD[Movie] = moviesRDD
    .filter(movie => movie.Major_Genre == "Drama" && movie.IMDB_Rating > 6.0)
  goodDrama.take(10).foreach(println)

  /**
   * Movie(First Love, Last Rites,Drama,Some(6.9))
   * Movie(12 Angry Men,Drama,Some(8.9))
   * Movie(Twelve Monkeys,Drama,Some(8.1))
   * Movie(1776,Drama,Some(7.0))
   * Movie(Twin Falls Idaho,Drama,Some(7.1))
   * Movie(55 Days at Peking,Drama,Some(6.8))
   * Movie(Amen,Drama,Some(7.4))
   * Movie(Barry Lyndon,Drama,Some(8.1))
   * Movie(Before Sunrise,Drama,Some(8.0))
   * Movie(The Best Years of Our Lives,Drama,Some(8.2))
   * */

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  private val groupByGenre: RDD[(String, Iterable[Movie])] = moviesRDD.groupBy(_.Major_Genre)
  private val genreAvgRating: RDD[GenreAvgRating] = groupByGenre.map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.IMDB_Rating).sum / movies.size)
  }
  genreAvgRating.take(10).foreach(println(_))
  /**
   * GenreAvgRating(Concert/Performance,6.325)
   * GenreAvgRating(Western,6.842857142857142)
   * GenreAvgRating(Musical,6.448)
   * GenreAvgRating(Horror,5.6760765550239185)
   * GenreAvgRating(Romantic Comedy,5.873076923076922)
   * GenreAvgRating(Comedy,5.853858267716529)
   * GenreAvgRating(Black Comedy,6.8187500000000005)
   * GenreAvgRating(Documentary,6.997297297297298)
   * GenreAvgRating(Adventure,6.345019920318729)
   * GenreAvgRating(Drama,6.773441734417339)
   * */

  // validation
  private val groupByGenreDS: RelationalGroupedDataset = moviesRDD.toDF().groupBy(col("Major_Genre"))
  private val dataFrame: DataFrame = groupByGenreDS.agg(avg(col("IMDB_Rating")))
  dataFrame.show()

  /**
   * +-------------------+------------------+
   * |        Major_Genre|  avg(IMDB_Rating)|
   * +-------------------+------------------+
   * |          Adventure| 6.345019920318729|
   * |              Drama| 6.773441734417339|
   * |        Documentary| 6.997297297297298|
   * |       Black Comedy|6.8187500000000005|
   * |  Thriller/Suspense| 6.359913793103447|
   * |            Musical|             6.448|
   * |    Romantic Comedy| 5.873076923076922|
   * |Concert/Performance|             6.325|
   * |             Horror|5.6760765550239185|
   * |            Western| 6.842857142857142|
   * |             Comedy| 5.853858267716529|
   * |             Action| 6.114795918367349|
   * +-------------------+------------------+ */

}
