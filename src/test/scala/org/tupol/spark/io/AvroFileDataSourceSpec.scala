package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.AvroSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class AvroFileDataSourceSpec extends FunSuite with Matchers with SharedSparkSession {

  test("The number of records in the file provided and the schema must match") {

    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val options = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(options, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF1 = FileDataSource(inputConfig).read

    resultDF1.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json")
    resultDF1.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read
    resultDF2.comapreWith(resultDF1).areEqual(true) shouldBe true
  }

  test("The number of records in the file provided and the other schema must match") {

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema-2.json")
    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val options = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(options, Some(expectedSchema))
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF1 = FileDataSource(inputConfig).read

    resultDF1.count shouldBe 3

    resultDF1.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read
    resultDF2.comapreWith(resultDF1).areEqual(true) shouldBe true

    resultDF1.schema.fields.map(_.name).foreach(println)
  }

}
