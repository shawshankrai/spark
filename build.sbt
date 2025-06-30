name := "spark-project"

version := "0.1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  // "com.amazonaws" % "AWSGlueETL_2.12" % "3.0.0" // Only needed on AWS Glue, not available on Maven Central
)

// Assembly settings for sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._

assembly / mainClass := Some("glue.GlueApp")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter { f =>
    f.data.getName.contains("spark-core") ||
    f.data.getName.contains("spark-sql") ||
    f.data.getName.contains("AWSGlueETL")
  }
}

// Exclude all files in the glue directory from local compilation
import sbt.io.HiddenFileFilter
import sbt._

Compile / unmanagedSources / excludeFilter := HiddenFileFilter || new SimpleFileFilter(file =>
  file.getAbsolutePath.contains("/glue/")
) 