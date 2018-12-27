package org.tupol.spark.io.parsers

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Success

class TextConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="text"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = ParserConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[TextConfiguration]

    converterConfig.get.schema.isDefined shouldBe false

  }
}
