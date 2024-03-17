package org.tupol.spark.io.pureconf.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.pureconf.SourceConfigurator
import org.tupol.spark.io.sources.GenericSourceConfiguration
import org.tupol.spark.sql.loadSchemaFromFile

import scala.util.Success

class GenericSourceConfigurationSpec extends AnyFunSuite with Matchers {

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json").get

  test("Parse configuration without options") {

    val expected = GenericSourceConfiguration(FormatType.Custom("custom_format"))

    val configStr = """
                      |format="custom_format"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)

  }

  test("Parse configuration with options") {

    val expectedOptions = Map("opt1" -> "yes", "opt2" -> "true", "opt3" -> "8")
    val expected        = GenericSourceConfiguration(FormatType.Custom("custom_format"), expectedOptions)

    val configStr = """
                      |format="custom_format"
                      |options={
                      |   opt1 : "yes"
                      |   opt2 : true
                      |   opt3 : 8
                      |}
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

  test("Parse configuration with options and schema") {

    val expectedOptions = Map("opt1" -> "yes", "opt2" -> "true", "opt3" -> "8")
    val expected =
      GenericSourceConfiguration(FormatType.Custom("custom_format"), expectedOptions, Some(ReferenceSchema))

    val configStr = """
                      |format="custom_format"
                      |options={
                      |   opt1 : "yes"
                      |   opt2 : true
                      |   opt3 : 8
                      |}
                      |schema.path: "sources/avro/sample_schema.json"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }
}
