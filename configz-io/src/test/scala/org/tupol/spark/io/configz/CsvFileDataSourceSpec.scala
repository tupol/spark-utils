package org.tupol.spark.io.configz

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.{FileDataSource, FileSourceConfiguration}
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.CsvSourceConfiguration
import org.tupol.spark.testing._

import scala.util.{Failure, Try}

class CsvFileDataSourceSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  test("The number of records in the csv provided must be the same in the output result") {

    val inputPath = "src/test/resources/sources/csv/cars.csv"
    val options = Map[String, String]()
    val header = true
    val delimiter = ","
    val parserConfig = CsvSourceConfiguration(options, None, delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)
    resultDF.count shouldBe csvDataFrame.count

    spark.source(inputConfig).read.get.compareWith(resultDF).areEqual(true) shouldBe true
  }

  test("The number of records in multiple csv files provided must be the same in the output result") {

    val header = true
    val delimiter = ","
    val inputPath = "src/test/resources/sources/csv/cars*.csv"
    val options = Map[String, String]()
    val parserConfig = CsvSourceConfiguration(options, None, delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get

    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)
    resultDF.count shouldBe csvDataFrame.count
  }

  test("User must be able to specify a schema ") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true),
      StructField("comment", StringType, true),
      StructField("blank", StringType, true)))
    val header = true
    val delimiter = ","
    val inputPath = "src/test/resources/sources/csv/cars.csv"
    val options = Map[String, String]()
    val parserConfig = CsvSourceConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.schema.fieldNames should contain("year")
    resultDF.schema.fieldNames should contain("make")
    resultDF.schema.fieldNames should contain("model")
    resultDF.schema.fieldNames should contain("comment")
    resultDF.schema.fieldNames should contain("blank")
  }

  test("Providing a good csv schema with the parsing option PERMISSIVE, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true),
      StructField("comment", StringType, true),
      StructField("blank", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "PERMISSIVE"
    val inputPath = "src/test/resources/sources/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvSourceConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count should be >= 0L
  }

  test("Providing a good csv schema with the parsing option DROPMALFORMED, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true),
      StructField("comment", StringType, true),
      StructField("blank", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "DROPMALFORMED"
    val inputPath = "src/test/resources/sources/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvSourceConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    val csvDataFrame = spark.read.format("com.databricks.spark.csv").schema(customSchema)
      .option("header", header).option("mode", mode).option("delimiter", delimiter).load(inputPath)

    resultDF.count should be <= csvDataFrame.count
  }

  test("Providing a good csv schema fitting perfectly the data with the parsing option FAILFAST, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "FAILFAST"
    val inputPath = "src/test/resources/sources/csv/cars_modified.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvSourceConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count should be >= 0L
  }

  test("Providing a wrong csv schema with the parsing option PERMISSIVE, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "PERMISSIVE"
    val inputPath = "src/test/resources/sources/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvSourceConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)
    resultDF.count should equal(csvDataFrame.count)
  }

  test("Providing a wrong csv schema with the parsing option DROPMALFORMED, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "DROPMALFORMED"
    val inputPath = "src/test/resources/sources/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvSourceConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)

    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count should be <= csvDataFrame.count
  }

  test("Providing a wrong csv schema with the parsing option FAILFAST, expecting a success run and a failed dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "FAILFAST"
    val inputPath = "src/test/resources/sources/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvSourceConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    Try(resultDF.collect) shouldBe a[Failure[_]]
  }
}
