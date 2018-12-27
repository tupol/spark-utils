package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.parsers.ParquetConfiguration
import org.tupol.spark.implicits._
import org.tupol.spark.sql._
import org.tupol.spark.testing._

class ParquetFileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession {

  test("The number of records in the file provided and the schema must match") {

    val inputPath = "src/test/resources/parsers/parquet/sample.parquet"
    val parserOptions = Map[String, String]()
    val parserConfig = ParquetConfiguration(parserOptions, None)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF = FileDataFrameLoader(inputConfig).loadData.get

    resultDF.count shouldBe 3

    val expectedSchema = loadSchemaFromFile("src/test/resources/parsers/parquet/sample_schema.json")
    resultDF.schema.fields.map(_.name) should contain allElementsOf (expectedSchema.fields.map(_.name))

    val resultDF2 = spark.loadData(inputConfig).get
    resultDF2.comapreWith(resultDF).areEqual(true) shouldBe true
  }

}
