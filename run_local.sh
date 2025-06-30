#!/bin/bash

# Exit on error
set -e

# Set JAVA_HOME to OpenJDK 17 for Spark 4.0.0 compatibility
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

# This script builds a fat JAR and runs the local Spark entry point (spark.Main) using spark-submit
# It does NOT run Glue-specific code (src/main/scala/glue), which requires AWS Glue libraries

# Build the project and create a fat JAR using Java 17
JAVA_HOME="$JAVA_HOME" sbt clean assembly

# Run the Spark Main class using spark-submit with common options and Java 17
JAVA_HOME="$JAVA_HOME" spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --conf "spark.ui.showConsoleProgress=false" \
  --class spark.Main \
  target/scala-2.12/spark-project-assembly-0.1.0.jar 