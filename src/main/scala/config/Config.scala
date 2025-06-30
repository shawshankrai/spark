package config

/**
 * Application configuration settings.
 */
object Config {
  /**
   * Example Spark configuration map.
   */
  val sparkConfigs: Map[String, String] = Map(
    "spark.sql.shuffle.partitions" -> "4",
    "spark.executor.memory" -> "2g"
  )
} 