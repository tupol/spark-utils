package org.tupol.spark.io.parsers

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Success

class CsvParserConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration with schema") {

    val configStr = """
                      |format="csv"
                      |header=true
                      |delimiter="DELIMITER"
                      |path="INPUT_PATH"
                      |
                      |schema  :{
                      |  "type" : "struct",
                      |  "fields" : [ {
                      |    "name" : "viewerId",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "asset",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "device/os",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "country",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "state",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "city",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "asn",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "isp",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "start time (unix time)",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "startup time (ms)",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "playing time (ms)",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "buffering time (ms)",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "interrupts",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "average bitrate (kbps)",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "startup error",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "session tags",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "ip address",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "cdn",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "browser",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "conviva session id",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "stream url",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "error list",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  },
                      |  {
                      |    "name" : "percentage complete",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  }
                      |  ]}
                      |
                      """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = ParserConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[CsvParserConfiguration]

    converterConfig.get.schema shouldBe a[Some[_]]

  }

  test("Parse configuration without schema") {

    val configStr = """
                      |format="csv"
                      |header=true
                      |delimiter="DELIMITER"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = ParserConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[CsvParserConfiguration]

    converterConfig.get.schema.isDefined shouldBe false

  }

  test("Parse configuration with options") {

    val configStr = """
                      |format="csv"
                      |path="INPUT_PATH"
                      |header=true
                      |delimiter="DELIMITER"
                      |options=[
                      |   {"mode" : "PERMISSIVE"},
                      |   {"samplingRatio" : "1"},
                      |   {"charset" : "UTF-8"},
                      |]
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = ParserConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[CsvParserConfiguration]

    converterConfig.get.options.isEmpty shouldBe false

    converterConfig.get.options shouldBe
      Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8",
        "header" -> "true", "delimiter" -> "DELIMITER")

  }

  test("Parse configuration with options overridden by top level properties") {

    val configStr = """
                      |format="csv"
                      |path="INPUT_PATH"
                      |header=true
                      |delimiter="DELIMITER"
                      |options=[
                      |   {"mode" : "PERMISSIVE"},
                      |   {"samplingRatio" : "1"},
                      |   {"charset" : "UTF-8"},
                      |   {"header" : "false"},
                      |   {"delimiter": "INNER_DELIMITER"}
                      |]
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = ParserConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[CsvParserConfiguration]

    converterConfig.get.options.isEmpty shouldBe false

    converterConfig.get.options shouldBe
      Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8",
        "header" -> "true", "delimiter" -> "DELIMITER")

  }

}
