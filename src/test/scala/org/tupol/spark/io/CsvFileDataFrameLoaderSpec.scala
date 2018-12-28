package org.tupol.spark.io

import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.parsers.CsvParserConfiguration
import org.tupol.spark.implicits._
import org.tupol.spark.testing._

import scala.util.{ Failure, Success, Try }

class CsvFileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession {

  test("The number of records in the csv provided must be the same in the output result") {

    val inputPath = "src/test/resources/parsers/csv/cars.csv"
    val options = Map[String, String]()
    val header = true
    val delimiter = ","
    val parserConfig = CsvParserConfiguration(options, None, delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData.get
    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)

    resultDF.count shouldBe csvDataFrame.count

    spark.loadData(inputConfig).get.comapreWith(resultDF).areEqual(true) shouldBe true
  }

  test("The number of records in multiple csv files provided must be the same in the output result") {

    val header = true
    val delimiter = ","
    val inputPath = "src/test/resources/parsers/csv/cars*.csv"
    val options = Map[String, String]()
    val parserConfig = CsvParserConfiguration(options, None, delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData
    resultDF shouldBe a[Success[_]]

    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)
    resultDF.get.count shouldBe csvDataFrame.count
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
    val inputPath = "src/test/resources/parsers/csv/cars.csv"
    val options = Map[String, String]()
    val parserConfig = CsvParserConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.schema.fieldNames should contain("year")
    resultDF.get.schema.fieldNames should contain("make")
    resultDF.get.schema.fieldNames should contain("model")
    resultDF.get.schema.fieldNames should contain("comment")
    resultDF.get.schema.fieldNames should contain("blank")
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
    val inputPath = "src/test/resources/parsers/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvParserConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count should be >= 0L
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
    val inputPath = "src/test/resources/parsers/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvParserConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData
    val csvDataFrame = spark.read.format("com.databricks.spark.csv").schema(customSchema)
      .option("header", header).option("mode", mode).option("delimiter", delimiter).load(inputPath)

    resultDF shouldBe a[Success[_]]

    resultDF.get.count should be <= csvDataFrame.count
  }

  test("Providing a good csv schema fitting perfectly the data with the parsing option FAILFAST, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "FAILFAST"
    val inputPath = "src/test/resources/parsers/csv/cars_modified.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvParserConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count should be >= 0L
  }

  test("Providing a wrong csv schema with the parsing option PERMISSIVE, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "PERMISSIVE"
    val inputPath = "src/test/resources/parsers/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvParserConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData
    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)

    resultDF shouldBe a[Success[_]]
    resultDF.get.count should equal(csvDataFrame.count)
  }

  test("Providing a wrong csv schema with the parsing option DROPMALFORMED, expecting a success run and a successful dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "DROPMALFORMED"
    val inputPath = "src/test/resources/parsers/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvParserConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)

    val csvDataFrame = spark.read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(inputPath)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count should be <= csvDataFrame.count
  }

  test("Providing a wrong csv schema with the parsing option FAILFAST, expecting a success run and a failed dataframe materialization") {

    val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true)))
    val header = true
    val delimiter = ","
    val mode = "FAILFAST"
    val inputPath = "src/test/resources/parsers/csv/cars.csv"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = CsvParserConfiguration(options, Some(customSchema), delimiter, header)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    Try(resultDF.get.collect) shouldBe a[Failure[_]]
  }
}
