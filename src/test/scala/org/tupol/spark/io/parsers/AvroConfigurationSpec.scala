package org.tupol.spark.io.parsers

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Success

class AvroConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="com.databricks.spark.avro"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = ParserConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[AvroConfiguration]

    converterConfig.get.schema.isDefined shouldBe false

  }
}
