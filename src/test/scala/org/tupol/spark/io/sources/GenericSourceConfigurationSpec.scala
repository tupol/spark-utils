package org.tupol.spark.io.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Success

class GenericSourceConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration without options") {

    val configStr = """
                      |format="custom_format"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfiguration(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[GenericSourceConfiguration]
    converterConfig.get.schema.isDefined shouldBe false

  }

  test("Parse configuration with options") {

    val configStr = """
                      |format="custom_format"
                      |options=[
                      |   {"opt1" : "yes"},
                      |   {"opt2" : true},
                      |   {"opt3" : 8},
                      |]
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfiguration(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[GenericSourceConfiguration]
    converterConfig.get.options.isEmpty shouldBe false

    converterConfig.get.options shouldBe
      Map("opt1" -> "yes", "opt2" -> "true", "opt3" -> "8")

  }
}
