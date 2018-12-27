package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.parsers.JsonParserConfiguration
import org.tupol.spark.implicits._
import org.tupol.spark.sql._
import org.tupol.spark.testing._

import scala.util.{ Failure, Success, Try }

class JsonFileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession {

  test("Extract from a single file with a single record should yield a single result") {

    val schema = loadSchemaFromFile("src/test/resources/parsers/json/sample_schema.json")
    val inputPath = "src/test/resources/parsers/json/sample_1line.json"
    val mode = "FAILFAST"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonParserConfiguration(parserOptions, Some(schema))
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData.get

    resultDF.count shouldBe 1

    val resultDF2 = spark.loadData(inputConfig).get
    resultDF2.comapreWith(resultDF).areEqual(true) shouldBe true
  }

  test("Extract from multiple files should yield as many results as the total number of records in the files") {

    val schema = loadSchemaFromFile("src/test/resources/parsers/json/sample_schema.json")
    val inputPath = "src/test/resources/parsers/json/ManyFiles/*.json"
    val mode = "FAILFAST"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonParserConfiguration(parserOptions, Some(schema))
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count shouldBe 2
  }

  test("Infer simple schema") {

    val schema = None
    val inputPath = "src/test/resources/parsers/json/sample.json"
    val mode = "PERMISSIVE"
    val parserOptions = Map[String, String]("mode" -> mode)
    val parserConfig = JsonParserConfiguration(parserOptions, schema)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    // Test that the field was inferred and the correct metadata was added

    val expectedSchema = loadSchemaFromFile("src/test/resources/parsers/json/sample_schema.json")
    resultDF.get.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    resultDF.get.count shouldBe 3
  }

  test("Deal with corrupted records in default mode (PERMISSIVE)") {

    val schema = loadSchemaFromFile("src/test/resources/parsers/json/sample_fail_schema.json")
    val inputPath = "src/test/resources/parsers/json/sample_fail.json"
    val mode = "PERMISSIVE"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonParserConfiguration(parserOptions, Some(schema))
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count shouldBe 4
  }

  test("Deal with corrupted records in PERMISSIVE mode with custom corrupt record column") {

    val columnNameOfCorruptRecord = "_customColumnNameOfCorruptRecord"

    val schema = Some(loadSchemaFromFile("src/test/resources/parsers/json/sample_fail_schema.json"))
    val inputPath = "src/test/resources/parsers/json/sample_fail.json"
    val mode = "PERMISSIVE"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> columnNameOfCorruptRecord, "mode" -> mode)
    val parserConfig = JsonParserConfiguration(parserOptions, schema)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count shouldBe 4
    resultDF.get.schema.fieldNames should contain(columnNameOfCorruptRecord)
  }

  test("Deal with corrupted records in DROPMALFORMED mode") {

    val schema = Some(loadSchemaFromFile("src/test/resources/parsers/json/sample_fail_schema.json"))
    val inputPath = "src/test/resources/parsers/json/sample_fail.json"
    val mode = "DROPMALFORMED"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonParserConfiguration(parserOptions, schema)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.collect.size shouldBe 2
    // TODO Investigate why the count does not match the expected result; e.g. in our case the collect.size != count
    // resultDF.get.count shouldBe 2
  }

  test("Deal with corrupted records in FAILFAST mode") {

    val schema = Some(loadSchemaFromFile("src/test/resources/parsers/json/sample_fail_schema.json"))
    val inputPath = "src/test/resources/parsers/json/sample_fail.json"
    val mode = "FAILFAST"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonParserConfiguration(parserOptions, schema)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    Try(resultDF.get.collect) shouldBe a[Failure[_]]
  }

}
