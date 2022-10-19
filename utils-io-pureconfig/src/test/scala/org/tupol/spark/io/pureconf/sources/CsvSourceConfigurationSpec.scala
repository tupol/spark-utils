package org.tupol.spark.io.pureconf.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.SourceConfigurator
import org.tupol.spark.io.sources.CsvSourceConfiguration
import org.tupol.spark.sql.loadSchemaFromFile

import scala.util.Success

class CsvSourceConfigurationSpec extends AnyFunSuite with Matchers {

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json").get

  test("Parse configuration with schema") {

    val expected = CsvSourceConfiguration(
      Map(
        "header" -> "true", "delimiter" -> "DELIMITER"),
      Some(ReferenceSchema)
    )

    val configStr = s"""
                      |format="csv"
                      |header=true
                      |delimiter="DELIMITER"
                      |path="INPUT_PATH"
                      |
                      |schema  : ${ReferenceSchema.prettyJson}
                      """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

  test("Parse configuration without schema") {

    val expected = CsvSourceConfiguration(
      Map(
        "header" -> "true", "delimiter" -> "DELIMITER")
    )

    val configStr = """
                      |format="csv"
                      |header=true
                      |delimiter="DELIMITER"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)

  }

  test("Parse configuration with options") {

    val expected = CsvSourceConfiguration(
      Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8",
        "header" -> "true", "delimiter" -> "DELIMITER")
    )

    val configStr = """
                      |format="csv"
                      |path="INPUT_PATH"
                      |header=true
                      |delimiter="DELIMITER"
                      |options {
                      |   mode : "PERMISSIVE"
                      |   samplingRatio : "1"
                      |   charset : "UTF-8"
                      |}
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

  test("Parse configuration with options overridden by top level properties") {

    val expected = CsvSourceConfiguration(
      Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8",
      "header" -> "true", "delimiter" -> "DELIMITER")
    )

    val configStr = """
                      |format="csv"
                      |path="INPUT_PATH"
                      |header=true
                      |delimiter="DELIMITER"
                      |options={
                      |   mode : "PERMISSIVE"
                      |   samplingRatio : "1"
                      |   charset : "UTF-8"
                      |   header : "false"
                      |   delimiter: "INNER_DELIMITER"
                      |}
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

}
