import sbt._

object Dependencies {

  object Versions {
    val scala = "2.12.12"
    val crossScala = Seq(scala)
    val scalaUtils = "1.0.1-SNAPSHOT"
    val scalaz = "7.2.26"
    val scalatest = "3.1.1"
    val scalacheck = "1.15.1"
    val json4s = "3.6.8"
    val scala_logging = "3.9.2"
    val mockito = "1.14.4"
    val typesafe_config = "1.4.0"
    val h2database = "1.4.197"

    val spark = "3.0.1"
    val sparkXml = "0.13.0"
    val fasterxml = "2.10.0"
    val embeddedKafka = "3.0.0"
  }

  val TestDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest % Test cross CrossVersion.binary,
    "org.scalacheck" %% "scalacheck" % Versions.scalacheck % Test cross CrossVersion.binary,
    "org.mockito" %% "mockito-scala" % Versions.mockito % Test cross CrossVersion.binary,
    "org.json4s" %% "json4s-core" % Versions.json4s % Test cross CrossVersion.binary,
    "org.json4s" %% "json4s-jackson" % Versions.json4s % Test cross CrossVersion.binary,
    "io.github.embeddedkafka" %% "embedded-kafka" % Versions.embeddedKafka % Test cross CrossVersion.binary,
    "org.apache.spark" %% "spark-avro" % Versions.spark % Test cross CrossVersion.binary,
    "com.databricks" %% "spark-xml" % Versions.sparkXml % Test cross CrossVersion.binary,
    "com.h2database" % "h2" % Versions.h2database % Test
  )

  val ProvidedSparkDependencies: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark force(),
    "org.apache.spark" %% "spark-sql" % Versions.spark force(),
    "org.apache.spark" %% "spark-mllib" % Versions.spark force(),
    "org.apache.spark" %% "spark-streaming" % Versions.spark force(),
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % Versions.spark
  ).map(_ % "provided")

  val AllDependencies: Seq[ModuleID] =  Seq(
    "org.tupol" %% "scala-utils" % Versions.scalaUtils
  ) ++ ProvidedSparkDependencies ++ TestDependencies


  // Jackson dependencies over Spark and Kafka Versions can be tricky; for Spark 3.0.x we need this override
  val FasterXmlOverrides: Seq[ModuleID] = Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.fasterxml,
    "com.fasterxml.jackson.core" % "jackson-core" % Versions.fasterxml,
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.fasterxml,
    "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.fasterxml,
    "com.fasterxml.jackson.module" % "jackson-module-paranamer" % Versions.fasterxml,
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % Versions.fasterxml,
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % Versions.fasterxml
  )

}
