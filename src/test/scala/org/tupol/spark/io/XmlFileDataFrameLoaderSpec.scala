package org.tupol.spark.io

import org.apache.spark.sql.types._
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.parsers.XmlParserConfiguration
import org.tupol.spark.implicits._
import org.tupol.spark.sql._
import org.tupol.spark.testing._

import scala.util.{ Failure, Success, Try }

class XmlFileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession {

  test("Extract the root element of a single file should yield a single result") {

    val schema = Some(loadSchemaFromFile("src/test/resources/parsers/xml/sample-schema.json"))
    val inputPath = "src/test/resources/parsers/xml/sample-1.xml"
    val mode = "PERMISSIVE"
    val rowTag = "root-name"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData.get

    resultDF.count shouldBe 1

    val resultDF2 = spark.loadData(inputConfig).get
    resultDF2.comapreWith(resultDF).areEqual(true) shouldBe true
  }

  test("Extract the root element of multiple files should yield as many results as the number of files") {

    val schema = Some(loadSchemaFromFile("src/test/resources/parsers/xml/sample-schema.json"))
    val inputPath = "src/test/resources/parsers/xml/sample-*.xml"
    val mode = "PERMISSIVE"
    val rowTag = "root-name"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count shouldBe 2
  }

  test("Extract elements that do not exist should return an empty result") {

    val schema = None
    val inputPath = "src/test/resources/parsers/xml/sample-1.xml"
    val mode = "PERMISSIVE"
    val rowTag = "tag_that_is_not_available"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count shouldBe 0

    println(resultDF.get.schema.prettyJson)
  }

  test("Infer simple schema") {

    val schema = None
    val inputPath = "src/test/resources/parsers/xml/test-corrupt-records.xml"
    val mode = "PERMISSIVE"
    val rowTag = "test_root"
    val parserOptions = Map[String, String]("mode" -> mode)
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    // Test that the field was inferred and the correct metadata was added
    resultDF.get.schema.fields should contain(
      new StructField("test_node", StringType, true)
    )

    resultDF.get.count shouldBe 13
  }

  test("Deal with corrupted records in default mode (PERMISSIVE)") {

    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/parsers/xml/test-corrupt-records.xml"
    val rowTag = "test_root"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record")
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count shouldBe 13
  }

  test("Deal with corrupted records in PERMISSIVE mode with custom corrupt record column") {

    val columnNameOfCorruptRecord = "_customColumnNameOfCorruptRecord"
    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/parsers/xml/test-corrupt-records.xml"
    val mode = "PERMISSIVE"
    val rowTag = "test_root"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> columnNameOfCorruptRecord, "mode" -> mode)
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.count shouldBe 13
    resultDF.get.schema.fieldNames should contain(columnNameOfCorruptRecord)
  }

  test("Deal with corrupted records in DROPMALFORMED mode") {

    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/parsers/xml/test-corrupt-records.xml"
    val mode = "DROPMALFORMED"
    val rowTag = "test_root"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    resultDF.get.collect.size shouldBe 10
  }

  test("Deal with corrupted records in FAILFAST mode") {

    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/parsers/xml/test-corrupt-records.xml"
    val mode = "FAILFAST"
    val rowTag = "test_root"
    val parserOptions = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlParserConfiguration(parserOptions, schema, rowTag)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData

    resultDF shouldBe a[Success[_]]
    Try(resultDF.get.collect) shouldBe a[Failure[_]]
  }

}
