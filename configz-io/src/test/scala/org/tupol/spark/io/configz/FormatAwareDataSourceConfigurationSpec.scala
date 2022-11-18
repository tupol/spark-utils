package org.tupol.spark.io.configz

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.configz._
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.sources.{GenericSourceConfiguration, JdbcSourceConfiguration, TextSourceConfiguration}
import org.tupol.spark.io.{FileSourceConfiguration, FormatAwareDataSourceConfiguration, FormatType}
import org.tupol.spark.sql.loadSchemaFromFile

class FormatAwareDataSourceConfigurationSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  val ReferenceSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json").get

  test("Successfully extract FileSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = TextSourceConfiguration())
    val result = config.extract[FormatAwareDataSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract GenericSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="my-format"
        |input.options={
        |  path: "INPUT_PATH"
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericSourceConfiguration(
      FormatType.Custom("my-format"),
      options = Map("path" -> "INPUT_PATH"),
      schema = None)

    val result = config.extract[FormatAwareDataSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Failed to extract FileSourceConfiguration if the path is not defined") {

    val configStr =
      """
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileSourceConfigurator.extract(config.getConfig("input"))

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSourceConfiguration if the format is not defined") {

    val configStr =
      """
        |input.path="INPUT_PATH"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[FormatAwareDataSourceConfiguration]("input")

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSourceConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[FormatAwareDataSourceConfiguration]("input")

    result.isSuccess shouldBe false
  }

  test("Successfully extract JdbcSourceConfiguration out of a configuration string") {

    val configStr =
      s"""
        |input.url="INPUT_URL"
        |input.table="SOURCE_TABLE"
        |input.user="USER_NAME"
        |input.password="USER_PASS"
        |input.driver="SOME_DRIVER"
        |input.options={
        |  opt1: "val1"
        |}
        |input.schema: ${ReferenceSchema.prettyJson}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = JdbcSourceConfiguration(
      url = "INPUT_URL",
      table = "SOURCE_TABLE",
      user = Some("USER_NAME"),
      password = Some("USER_PASS"),
      driver = Some("SOME_DRIVER"),
      options = Map("opt1" -> "val1"),
      schema = Some(ReferenceSchema))
    val result = config.extract[FormatAwareDataSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract JdbcSourceConfiguration out of a configuration string containing only mandatory fields") {

    val configStr =
      """
        |input.url="INPUT_URL"
        |input.table="SOURCE_TABLE"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = JdbcSourceConfiguration(
      url = "INPUT_URL",
      table = "SOURCE_TABLE",
      user = None,
      password = None,
      driver = None,
      options = Map[String, String](),
      schema = None)
    val result = config.extract[FormatAwareDataSourceConfiguration]("input")

    result.get shouldBe expected
  }

}
