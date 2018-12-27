package org.tupol.spark.io.source

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.sources.{ AvroSourceConfiguration, SourceConfiguration }

import scala.util.Success

class AvroSourceConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="com.databricks.spark.avro"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = SourceConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[AvroSourceConfiguration]

    converterConfig.get.schema.isDefined shouldBe false

  }
}
