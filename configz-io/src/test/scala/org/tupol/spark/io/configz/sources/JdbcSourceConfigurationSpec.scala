package org.tupol.spark.io.configz.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.sources.JdbcSourceConfiguration

import scala.util.Success

class JdbcSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="jdbc"
                      |url="OUTPUT_URL"
                      |table="SOURCE_TABLE"
                    """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfigurator.extract(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[JdbcSourceConfiguration]
    converterConfig.get.schema.isDefined shouldBe false

  }
}
