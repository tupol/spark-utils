package org.tupol.spark.io.source

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.sources.{ OrcSourceConfiguration, SourceConfiguration }

import scala.util.Success

class OrcSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="orc"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfiguration.extract(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[OrcSourceConfiguration]
    converterConfig.get.schema.isDefined shouldBe false

  }
}
