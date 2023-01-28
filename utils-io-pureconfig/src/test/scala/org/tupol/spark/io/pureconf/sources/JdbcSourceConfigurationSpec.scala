package org.tupol.spark.io.pureconf.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.SourceConfigurator
import org.tupol.spark.io.sources.JdbcSourceConfiguration
import org.tupol.spark.sql.loadSchemaFromFile

import scala.util.Success

class JdbcSourceConfigurationSpec extends AnyFunSuite with Matchers {

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json").get

  test("Parse configuration without schema") {

    val configStr = """
                      |format="jdbc"
                      |url="OUTPUT_URL"
                      |table="SOURCE_TABLE"
                      |user=test_user
                      |password="some_pass"
                      |driver=jdbc.driver
                      |options: {
                      | option1: option_1_value
                      | option2: option_2_value
                      |}
                    """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    val expectedOptions = Map(
        "url" -> "OUTPUT_URL",
        "dbtable" -> "SOURCE_TABLE",
        "user" -> "test_user",
        "password" -> "some_pass",
        "driver" -> "jdbc.driver",
        "option1" -> "option_1_value",
        "option2" -> "option_2_value"
    )

    val expected = JdbcSourceConfiguration(expectedOptions, None)

    result shouldBe Success(expected)
  }

  test("Parse configuration with path schema") {

    val configStr = """
                      |format="jdbc"
                      |url="OUTPUT_URL"
                      |table="SOURCE_TABLE"
                      |user=test_user
                      |password="some_pass"
                      |driver=jdbc.driver
                      |options: {
                      | option1: option_1_value
                      | option2: option_2_value
                      |}
                      |schema.path: "sources/avro/sample_schema.json"
                    """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    val expectedOptions = Map(
      "url" -> "OUTPUT_URL",
      "dbtable" -> "SOURCE_TABLE",
      "user" -> "test_user",
      "password" -> "some_pass",
      "driver" -> "jdbc.driver",
      "option1" -> "option_1_value",
      "option2" -> "option_2_value"
    )

    val expected = JdbcSourceConfiguration(expectedOptions, Some(ReferenceSchema))

    result shouldBe Success(expected)
  }

  test("Parse configuration with explicit schema") {

    val configStr = s"""
                      |format="jdbc"
                      |url="OUTPUT_URL"
                      |table="SOURCE_TABLE"
                      |user=test_user
                      |password="some_pass"
                      |driver=jdbc.driver
                      |options: {
                      | option1: option_1_value
                      | option2: option_2_value
                      |}
                      |schema: ${ReferenceSchema.prettyJson}
                    """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    val expectedOptions = Map(
      "url" -> "OUTPUT_URL",
      "dbtable" -> "SOURCE_TABLE",
      "user" -> "test_user",
      "password" -> "some_pass",
      "driver" -> "jdbc.driver",
      "option1" -> "option_1_value",
      "option2" -> "option_2_value"
    )

    val expected = JdbcSourceConfiguration(expectedOptions, Some(ReferenceSchema))

    result shouldBe Success(expected)
  }
}
