package org.tupol.spark.io

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.ParquetSourceConfiguration
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class ParquetFileDataSourceSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  test("The number of records in the file provided and the schema must match") {

    val inputPath    = "src/test/resources/sources/parquet/sample.parquet"
    val options      = Map[String, String]()
    val parserConfig = ParquetSourceConfiguration(options, None)
    val inputConfig  = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF     = FileDataSource(inputConfig).read.get

    resultDF.count() shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/sources/parquet/sample_schema.json").get
    resultDF.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.source(inputConfig).read.get
    resultDF2.compareWith(resultDF).areEqual(true) shouldBe true
  }

}
