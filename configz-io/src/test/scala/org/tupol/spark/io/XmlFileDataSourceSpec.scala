package org.tupol.spark.io

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.XmlSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

import scala.util.{Failure, Try}

class XmlFileDataSourceSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  test("Extract the root element of a single file should yield a single result") {

    val schema = Some(loadSchemaFromFile("src/test/resources/sources/xml/sample-schema.json").get)
    val inputPath = "src/test/resources/sources/xml/sample-1.xml"
    val mode = "PERMISSIVE"
    val rowTag = "root-name"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get

    resultDF.count shouldBe 1

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF).areEqual(true) shouldBe true
  }

  test("Extract the root element of multiple files should yield as many results as the number of files") {

    val schema = Some(loadSchemaFromFile("src/test/resources/sources/xml/sample-schema.json").get)
    val inputPath = "src/test/resources/sources/xml/sample-*.xml"
    val mode = "PERMISSIVE"
    val rowTag = "root-name"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count shouldBe 2
  }

  test("Extract elements that do not exist should return an empty result") {

    val schema = None
    val inputPath = "src/test/resources/sources/xml/sample-1.xml"
    val mode = "PERMISSIVE"
    val rowTag = "tag_that_is_not_available"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count shouldBe 0
  }

  test("Infer simple schema") {

    val schema = None
    val inputPath = "src/test/resources/sources/xml/test-corrupt-records.xml"
    val mode = "PERMISSIVE"
    val rowTag = "test_root"
    val options = Map[String, String]("mode" -> mode)
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    // Test that the field was inferred and the correct metadata was added
    resultDF.schema.fields should contain(new StructField("test_node", StringType, true))

    resultDF.count shouldBe 13
  }

  test("Deal with corrupted records in default mode (PERMISSIVE)") {

    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/sources/xml/test-corrupt-records.xml"
    val rowTag = "test_root"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record")
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count shouldBe 13
  }

  test("Deal with corrupted records in PERMISSIVE mode with custom corrupt record column") {

    val columnNameOfCorruptRecord = "_customColumnNameOfCorruptRecord"
    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/sources/xml/test-corrupt-records.xml"
    val mode = "PERMISSIVE"
    val rowTag = "test_root"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> columnNameOfCorruptRecord, "mode" -> mode)
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.count shouldBe 13
    resultDF.schema.fieldNames should contain(columnNameOfCorruptRecord)
  }

  test("Deal with corrupted records in DROPMALFORMED mode") {

    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/sources/xml/test-corrupt-records.xml"
    val mode = "DROPMALFORMED"
    val rowTag = "test_root"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    resultDF.collect.size shouldBe 10
  }

  test("Deal with corrupted records in FAILFAST mode") {

    val schema = Some(new StructType().add("test_node", LongType))
    val inputPath = "src/test/resources/sources/xml/test-corrupt-records.xml"
    val mode = "FAILFAST"
    val rowTag = "test_root"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = XmlSourceConfiguration(options, schema, rowTag)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get
    Try(resultDF.collect) shouldBe a[Failure[_]]
  }

}
