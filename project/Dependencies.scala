import sbt._

object Dependencies {

  object Versions {
    val jvm = "1.8"
    val targetJava = "8"
    val scala_2_12 = "2.12.19"
    val scala_2_13 = "2.13.14"
    val scala = scala_2_13
    val crossScala = Seq(scala_2_12, scala_2_13)
    val scalaUtils = "2.0.0"
    val scalatest = "3.2.18"
    val scalacheck = "1.18.0"
    val json4s = "3.6.8"
    val scalaLogging = "3.9.2"
    val mockito = "1.14.4"
    val typesafeConfig = "1.4.3"
    val h2database = "2.2.224"
    val pureconfig = "0.17.6"

    val spark = "3.3.0"
    val sparkXml = "0.17.0"
    val fasterxml = "2.12.3"
    val embeddedKafka = "3.7.0"
    val snappy = "1.1.8.4"
  }

  val CoreTestDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest % Test cross CrossVersion.binary,
    "org.scalacheck" %% "scalacheck" % Versions.scalacheck % Test cross CrossVersion.binary,
    "org.mockito" %% "mockito-scala" % Versions.mockito % Test cross CrossVersion.binary,
    "org.apache.spark" %% "spark-core" % Versions.spark force(),
    "org.apache.spark" %% "spark-sql" % Versions.spark force(),
    "org.apache.spark" %% "spark-avro" % Versions.spark % Test cross CrossVersion.binary,
    "com.databricks" %% "spark-xml" % Versions.sparkXml % Test cross CrossVersion.binary,
    "com.h2database" % "h2" % Versions.h2database % Test,
    "org.xerial.snappy" % "snappy-java" % Versions.snappy % Test
  )

  val IoTestDependencies: Seq[ModuleID] = Seq(
    "org.json4s" %% "json4s-core" % Versions.json4s % Test cross CrossVersion.binary,
    "org.json4s" %% "json4s-jackson" % Versions.json4s % Test cross CrossVersion.binary,
    "io.github.embeddedkafka" %% "embedded-kafka" % Versions.embeddedKafka % Test cross CrossVersion.binary
  )

  val ProvidedSparkCoreDependencies: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark force(),
    "org.apache.spark" %% "spark-sql" % Versions.spark force(),
    "org.apache.spark" %% "spark-mllib" % Versions.spark force(),
    "org.apache.spark" %% "spark-streaming" % Versions.spark force()
  ).map(_ % "provided")

  val ProvidedSparkKafkaDependencies: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % Versions.spark
  ).map(_ % "provided")

  val CoreDependencies: Seq[ModuleID] =  Seq(
    "org.tupol" %% "scala-utils-core" % Versions.scalaUtils,
    "com.typesafe" % "config" % Versions.typesafeConfig
  )

  val IoConfigzDependencies: Seq[ModuleID] =  Seq(
    "org.tupol" %% "scala-utils-config-z" % Versions.scalaUtils
  )

  val IoPureconfigDependencies: Seq[ModuleID] =  Seq(
    "com.github.pureconfig" %% "pureconfig" % Versions.pureconfig
  )

  // Jackson dependencies over Spark and Kafka Versions can be tricky; for Spark 3.0.x we need this override; Not required for spark 3.5
  val FasterXmlOverrides: Seq[ModuleID] = Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.fasterxml,
    "com.fasterxml.jackson.core" % "jackson-core" % Versions.fasterxml,
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.fasterxml,
    "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.fasterxml,
    "com.fasterxml.jackson.module" % "jackson-module-paranamer" % Versions.fasterxml,
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % Versions.fasterxml,
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % Versions.fasterxml
  )

  // Override needed for Spark 3.2.x; Not required for spark 3.5
  val NettyOverrides: Seq[ModuleID] = Seq(
    "io.netty" % "netty-buffer" % "4.1.63.Final"
  )

}
