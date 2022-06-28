package org.tupol.spark.io.pureconf.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.SourceConfigurator
import org.tupol.spark.io.sources.AvroSourceConfiguration

import scala.util.Success

class AvroSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="com.databricks.spark.avro"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val result = SourceConfigurator.extract(config)

    result shouldBe a[Success[_]]
    result.get shouldBe a[AvroSourceConfiguration]
    result.get.schema.isDefined shouldBe false

  }
}
