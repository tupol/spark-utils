package org.tupol.spark.io

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.{AvroSourceConfiguration, GenericSourceConfiguration}
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class GenericDataSourceSpec extends AnyWordSpec with Matchers with SharedSparkSession {

  val CustomFormat = FormatType.Custom("com.databricks.spark.avro")

  "Loading the data fails if the file does not exist" in {

    val inputPath = "unknown/path/to/inexistent/file.no.way"
    val options = Map[String, String]("path" -> inputPath)
    val inputConfig = GenericSourceConfiguration(CustomFormat, options)

    a[DataSourceException] should be thrownBy GenericDataSource(inputConfig).read.get

    a[DataSourceException] should be thrownBy spark.source(inputConfig).read.get
  }

  "The number of records in the file provided and the schema must match" in {

    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val options = Map[String, String]("path" -> inputPath)
    val inputConfig = GenericSourceConfiguration(CustomFormat, options)
    val resultDF1 = spark.source(inputConfig).read.get

    resultDF1.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json").get
    resultDF1.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF1).areEqual(true) shouldBe true
  }

  "The number of records in the file provided and the other schema must match" in {

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema-2.json").get
    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val options = Map[String, String]("path" -> inputPath)
    val inputConfig = GenericSourceConfiguration(CustomFormat, options, Some(expectedSchema))
    val resultDF1 = GenericDataSource(inputConfig).read.get

    resultDF1.count shouldBe 3

    resultDF1.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF1).areEqual(true) shouldBe true
  }

  "addOptions" should {
    "leave the empty options unchanged" in {
      val options = Map[String, String]()
      val extraOptions = Map[String, String]()
      val result = GenericSourceConfiguration(CustomFormat, options, None).addOptions(extraOptions)
      result.options should contain theSameElementsAs options
    }
    "leave the options unchanged" in {
      val options = Map("key1" -> "value1", "key2" -> "value2")
      val extraOptions = Map[String, String]()
      val result = GenericSourceConfiguration(CustomFormat, options, None).addOptions(extraOptions)
      result.options should contain theSameElementsAs options
    }
    "add extra options" in {
      val options = Map("key1" -> "value1", "key2" -> "value2")
      val extraOptions = Map("key3" -> "value3")
      val expectedOptions = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
      val result = GenericSourceConfiguration(CustomFormat, options, None).addOptions(extraOptions)
      result.options should contain theSameElementsAs expectedOptions
    }
    "override options" in {
      val options = Map("key1" -> "value1", "key2" -> "value2")
      val extraOptions = Map("key2" -> "value3")
      val expectedOptions = Map("key1" -> "value1", "key2" -> "value3")
      val result = GenericSourceConfiguration(CustomFormat, options, None).addOptions(extraOptions)
      result.options should contain theSameElementsAs expectedOptions
    }
  }

  "withSchema" should {
    val oldSchema = new StructType().add("field1", StringType)
    val newSchema = new StructType().add("field2", LongType)

    "set a new schema" in {
      val options = Map[String, String]()
      val result = GenericSourceConfiguration(CustomFormat, options, None).withSchema(Some(newSchema))
      result.schema shouldBe Some(newSchema)
    }
    "change an existing schema" in {
      val options = Map[String, String]()
      val result = GenericSourceConfiguration(CustomFormat, options, Some(oldSchema)).withSchema(Some(newSchema))
      result.schema shouldBe Some(newSchema)
    }
    "remove an existing schema" in {
      val options = Map[String, String]()
      val result = GenericSourceConfiguration(CustomFormat, options, Some(oldSchema)).withSchema(None)
      result.schema shouldBe None
    }
  }

}
