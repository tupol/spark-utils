package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.parsers.OrcConfiguration
import org.tupol.spark.implicits._
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class OrcFileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession {

  override def sparkConfig = super.sparkConfig + ("spark.sql.orc.impl" -> "native")

  test("The number of records in the file provided and the schema must match") {

    val inputPath = "src/test/resources/parsers/orc/sample.orc"
    val parserOptions = Map[String, String]()
    val parserConfig = OrcConfiguration(parserOptions, None)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData.get

    resultDF.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/parsers/orc/sample_schema.json")
    resultDF.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    spark.loadData(inputConfig).get.comapreWith(resultDF).areEqual(true) shouldBe true
  }

}
