package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.OrcSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class OrcFileDataSourceSpec extends FunSuite with Matchers with SharedSparkSession {

  override def sparkConfig = super.sparkConfig + ("spark.sql.orc.impl" -> "native")

  test("The number of records in the file provided and the schema must match") {

    val inputPath = "src/test/resources/sources/orc/sample.orc"
    val options = Map[String, String]()
    val parserConfig = OrcSourceConfiguration(options, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF = FileDataSource(inputConfig).read.get

    resultDF.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/orc/sample_schema.json")
    resultDF.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    spark.source(inputConfig).read.get.comapreWith(resultDF).areEqual(true) shouldBe true
  }

}
