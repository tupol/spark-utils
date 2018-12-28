package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.parsers.AvroConfiguration
import org.tupol.spark.implicits._
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class AvroFileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession {

  test("The number of records in the file provided and the schema must match") {

    val inputPath = "src/test/resources/parsers/avro/sample.avro"
    val options = Map[String, String]()
    val parserConfig = AvroConfiguration(options, None)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF1 = FileDataFrameLoader(inputConfig).loadData.get

    resultDF1.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/parsers/avro/sample_schema.json")
    resultDF1.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.loadData(inputConfig).get
    resultDF2.comapreWith(resultDF1).areEqual(true) shouldBe true
  }

}
