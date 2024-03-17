package org.tupol.spark.io.pureconf.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.sources.XmlSourceConfiguration
import org.tupol.spark.sql.loadSchemaFromFile

import scala.util.Success

class XmlSourceConfigurationSpec extends AnyFunSuite with Matchers {

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json").get

  import org.tupol.spark.io.pureconf._

  test("Parse configuration with schema") {

    val expected = XmlSourceConfiguration(Map(), Some(ReferenceSchema), "ROW_TAG")

    val configStr = s"""
                       |format="com.databricks.spark.xml"
                       |path="INPUT_PATH"
                       |rowTag="ROW_TAG"
                       |
                       |schema: ${ReferenceSchema.prettyJson}
                       |""".stripMargin

    val config = ConfigFactory.parseString(configStr)

    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

  test("Parse configuration without schema") {

    val expected = XmlSourceConfiguration(Map("rowTag" -> "ROW_TAG"))

    val configStr = """
                      |format="com.databricks.spark.xml"
                      |path="INPUT_PATH"
                      |rowTag="ROW_TAG"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

  test("Parse configuration with options") {

    val expected = XmlSourceConfiguration(
      Map("rowTag" -> "ROW_TAG", "mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8")
    )

    val configStr = """
                      |format="com.databricks.spark.xml"
                      |path="INPUT_PATH"
                      |rowTag="ROW_TAG"
                      |options = {
                      |  mode : "PERMISSIVE"
                      |  samplingRatio : "1"
                      |  charset : "UTF-8"
                      |}
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

  test("Parse configuration with options overridden by top level properties") {

    val expected = XmlSourceConfiguration(
      Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8", "rowTag" -> "ROW_TAG")
    )

    val configStr = """
                      |format="com.databricks.spark.xml"
                      |path="INPUT_PATH"
                      |rowTag="ROW_TAG"
                      |options={
                      |   mode : "PERMISSIVE"
                      |   samplingRatio : "1"
                      |   charset : "UTF-8"
                      |   rowTag: "INNER_ROW_TAG"
                      |}
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe Success(expected)
  }

}
