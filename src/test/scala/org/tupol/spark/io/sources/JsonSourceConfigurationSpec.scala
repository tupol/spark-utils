package org.tupol.spark.io.source

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.sources.{ JsonSourceConfiguration, SourceConfiguration }

import scala.util.Success

class JsonSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Parse configuration with schema") {

    val configStr = """
                      |format="json"
                      |path="INPUT_PATH"
                      |mode="FAILFAST"
                      |schema  :{
                      |  "type" : "struct",
                      |  "fields" : [ {
                      |    "name" : "cpeid",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  }, {
                      |    "name" : "creation_time",
                      |    "type" : "string",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  }, {
                      |    "name" : "props.Event",
                      |    "type" : {
                      |      "type" : "array",
                      |      "elementType" : {
                      |        "type" : "struct",
                      |        "fields" : [ {
                      |          "name" : "Class",
                      |          "type" : "string",
                      |          "nullable" : true,
                      |          "metadata" : { }
                      |        }, {
                      |          "name" : "Details",
                      |          "type" : {
                      |            "type" : "struct",
                      |            "fields" : [ {
                      |              "name" : "channel_id",
                      |              "type" : "string",
                      |              "nullable" : true,
                      |              "metadata" : { }
                      |            }, {
                      |              "name" : "data",
                      |              "type" : {
                      |                "type" : "struct",
                      |                "fields" : [ {
                      |                  "name" : "keycode",
                      |                  "type" : "long",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "previous_id",
                      |                  "type" : "string",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                } ]
                      |              },
                      |              "nullable" : true,
                      |              "metadata" : { }
                      |            }, {
                      |              "name" : "id",
                      |              "type" : {
                      |                "type" : "struct",
                      |                "fields" : [ {
                      |                  "name" : "event_id",
                      |                  "type" : "string",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                } ]
                      |              },
                      |              "nullable" : true,
                      |              "metadata" : { }
                      |            }, {
                      |              "name" : "reason",
                      |              "type" : "string",
                      |              "nullable" : true,
                      |              "metadata" : { }
                      |            }, {
                      |              "name" : "screen_id",
                      |              "type" : "string",
                      |              "nullable" : true,
                      |              "metadata" : { }
                      |            }, {
                      |              "name" : "type",
                      |              "type" : "string",
                      |              "nullable" : true,
                      |              "metadata" : { }
                      |            } ]
                      |          },
                      |          "nullable" : true,
                      |          "metadata" : { }
                      |        }, {
                      |          "name" : "TS",
                      |          "type" : "string",
                      |          "nullable" : true,
                      |          "metadata" : { }
                      |        }, {
                      |          "name" : "Type",
                      |          "type" : "string",
                      |          "nullable" : true,
                      |          "metadata" : { }
                      |        } ]
                      |      },
                      |      "containsNull" : true
                      |    },
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  }, {
                      |    "name" : "props.numEntries",
                      |    "type" : "long",
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  } ]
                      |}
                      |
                      """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfiguration.extract(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[JsonSourceConfiguration]
    converterConfig.get.schema shouldBe a[Some[_]]

  }

  test("Parse configuration without schema") {

    val configStr = """
                      |format="json"
                      |path="INPUT_PATH"
                      |mode="FAILFAST"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfiguration.extract(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[JsonSourceConfiguration]
    converterConfig.get.schema.isDefined shouldBe false

  }

  test("Parse configuration with options") {

    val configStr = """
                      |format="json"
                      |path="INPUT_PATH"
                      |options=[
                      |   {"mode" : "PERMISSIVE"},
                      |   {"columnNameOfCorruptRecord" : "_arbitrary_name"}
                      |]
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfiguration.extract(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[JsonSourceConfiguration]
    converterConfig.get.options.isEmpty shouldBe false
    converterConfig.get.options should contain
    theSameElementsAs(Map("mode" -> "PERMISSIVE", "columnNameOfCorruptRecord" -> "_arbitrary_name"))

  }

}
