package org.tupol.spark.io.configz

import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.{FileDataSource, FileSourceConfiguration}
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.JsonSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

import scala.util.{Failure, Try}

class JsonFileDataSourceSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  test("Extract from a single file with a single record should yield a single result") {

    val schema = loadSchemaFromFile("src/test/resources/sources/json/sample_schema.json").get
    val inputPath = "src/test/resources/sources/json/sample_1line.json"
    val mode = "FAILFAST"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, Some(schema))
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get

    resultDF.count shouldBe 1

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF).areEqual(true) shouldBe true
  }

  test("Extract from multiple files should yield as many results as the total number of records in the files") {

    val schema = loadSchemaFromFile("src/test/resources/sources/json/sample_schema.json").get
    val inputPath = "src/test/resources/sources/json/ManyFiles/*.json"
    val mode = "FAILFAST"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, Some(schema))
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count shouldBe 2
  }

  test("Infer simple schema") {

    val schema = None
    val inputPath = "src/test/resources/sources/json/sample.json"
    val mode = "PERMISSIVE"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, schema)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    // Test that the field was inferred and the correct metadata was added

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/json/sample_schema.json").get
    resultDF.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    resultDF.count shouldBe 3
  }

  test("Deal with corrupted records in default mode (PERMISSIVE)") {

    val schema = loadSchemaFromFile("src/test/resources/sources/json/sample_fail_schema.json").get
    val inputPath = "src/test/resources/sources/json/sample_fail.json"
    val mode = "PERMISSIVE"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, Some(schema))
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count shouldBe 4
  }

  test("Deal with corrupted records in PERMISSIVE mode with custom corrupt record column") {

    val columnNameOfCorruptRecord = "_customColumnNameOfCorruptRecord"

    val schema = Some(loadSchemaFromFile("src/test/resources/sources/json/sample_fail_schema.json").get)
    val inputPath = "src/test/resources/sources/json/sample_fail.json"
    val mode = "PERMISSIVE"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> columnNameOfCorruptRecord, "mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, schema)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count shouldBe 4
    resultDF.schema.fieldNames should contain(columnNameOfCorruptRecord)
  }

  test("Deal with corrupted records in DROPMALFORMED mode") {

    val schema = Some(loadSchemaFromFile("src/test/resources/sources/json/sample_fail_schema.json").get)
    val inputPath = "src/test/resources/sources/json/sample_fail.json"
    val mode = "DROPMALFORMED"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, schema)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.collect.size shouldBe 2
    // TODO Investigate why the count does not match the expected result; e.g. in our case the collect.size != count
    // resultDF.count shouldBe 2
  }

  test("Deal with corrupted records in FAILFAST mode") {

    val schema = Some(loadSchemaFromFile("src/test/resources/sources/json/sample_fail_schema.json").get)
    val inputPath = "src/test/resources/sources/json/sample_fail.json"
    val mode = "FAILFAST"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, schema)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    Try(resultDF.collect) shouldBe a[Failure[_]]
  }

}
