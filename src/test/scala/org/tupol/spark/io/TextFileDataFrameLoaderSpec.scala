package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.parsers.TextConfiguration
import org.tupol.spark.implicits._
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class TextFileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession {

  test("The number of records in the file provided and the schema must match") {
    import spark.implicits._
    val inputPath = "src/test/resources/parsers/text/sample.txt"
    val options = Map[String, String]()
    val parserConfig = TextConfiguration(options, None)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData.get

    resultDF.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/parsers/text/sample_schema.json")
    spark.read.json(resultDF.as[String]).schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.loadData(inputConfig).get
    resultDF2.comapreWith(resultDF).areEqual(true) shouldBe true
  }

}
