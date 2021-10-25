package org.tupol.spark.io.configz

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.AvroSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class AvroFileDataSourceSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  test("The number of records in the file provided and the schema must match") {

    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val options = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(options, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF1 = FileDataSource(inputConfig).read.get

    resultDF1.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json").get
    resultDF1.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF1).areEqual(true) shouldBe true
  }

  test("The number of records in the file provided and the other schema must match") {

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema-2.json").get
    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val options = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(options, Some(expectedSchema))
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF1 = FileDataSource(inputConfig).read.get

    resultDF1.count shouldBe 3

    resultDF1.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF1).areEqual(true) shouldBe true
  }

}
