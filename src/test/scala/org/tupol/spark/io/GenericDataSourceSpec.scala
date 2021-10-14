package org.tupol.spark.io

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.GenericSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class GenericDataSourceSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  val CustomFormat = FormatType.Custom("com.databricks.spark.avro")

  test("Loading the data fails if the file does not exist") {

    val inputPath = "unknown/path/to/inexistent/file.no.way"
    val options = Map[String, String]("path" -> inputPath)
    val inputConfig = GenericSourceConfiguration(CustomFormat, options)

    a[DataSourceException] should be thrownBy GenericDataSource(inputConfig).read.get

    a[DataSourceException] should be thrownBy spark.source(inputConfig).read.get
  }

  test("The number of records in the file provided and the schema must match") {

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

  test("The number of records in the file provided and the other schema must match") {

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

}
