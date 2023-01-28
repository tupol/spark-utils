package org.tupol.spark.io.configz.source

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.configz.sources.SourceConfigurator
import org.tupol.spark.io.sources.ParquetSourceConfiguration

import scala.util.Success

class ParquetSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Parse configuration without schema") {

    val configStr = """
                      |format="parquet"
                      |path="INPUT_PATH"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    val converterConfig = SourceConfigurator.extract(config)

    converterConfig shouldBe a[Success[_]]
    converterConfig.get shouldBe a[ParquetSourceConfiguration]
    converterConfig.get.schema.isDefined shouldBe false

  }
}
