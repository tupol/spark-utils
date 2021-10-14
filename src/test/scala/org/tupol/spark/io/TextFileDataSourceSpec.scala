package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.TextSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class TextFileDataSourceSpec extends FunSuite with Matchers with SharedSparkSession {

  test("The number of records in the file provided and the schema must match") {
    import spark.implicits._
    val inputPath = "src/test/resources/sources/text/sample.txt"
    val options = Map[String, String]()
    val parserConfig = TextSourceConfiguration(options, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get

    resultDF.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/text/sample_schema.json")
    spark.read.json(resultDF.as[String]).schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF).areEqual(true) shouldBe true
  }

}
