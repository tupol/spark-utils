package org.tupol.spark.io.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class JdbcSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="jdbc"
                      |url="OUTPUT_URL"
                      |table="SOURCE_TABLE"
                    """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfiguration.extract(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[JdbcSourceConfiguration]
    converterConfig.get.schema.isDefined shouldBe false

  }
}
