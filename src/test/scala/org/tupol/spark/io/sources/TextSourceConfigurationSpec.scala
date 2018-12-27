package org.tupol.spark.io.source

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.sources.{ SourceConfiguration, TextSourceConfiguration }

import scala.util.Success

class TextSourceConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="text"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = SourceConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[TextSourceConfiguration]

    converterConfig.get.schema.isDefined shouldBe false

  }
}
