package org.tupol.spark.io.source

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.sources.{ ParquetSourceConfiguration, SourceConfiguration }

import scala.util.Success

class ParquetSourceConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="parquet"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val converterConfig = SourceConfiguration(config)

    converterConfig shouldBe a[Success[_]]

    converterConfig.get shouldBe a[ParquetSourceConfiguration]

    converterConfig.get.schema.isDefined shouldBe false

  }
}
