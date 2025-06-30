package config

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for Config object.
 */
class ConfigTest extends AnyFunSuite {
  test("sparkConfigs should contain expected keys") {
    assert(Config.sparkConfigs.contains("spark.sql.shuffle.partitions"))
    assert(Config.sparkConfigs.contains("spark.executor.memory"))
  }
} 