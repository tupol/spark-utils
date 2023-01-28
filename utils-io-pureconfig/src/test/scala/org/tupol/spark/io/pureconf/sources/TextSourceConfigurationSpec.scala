package org.tupol.spark.io.pureconf.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.SourceConfigurator
import org.tupol.spark.io.sources.TextSourceConfiguration

import scala.util.Success

class TextSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="text"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe a[Success[_]]
    result.get shouldBe a[TextSourceConfiguration]
    result.get.schema.isDefined shouldBe false

  }
}
