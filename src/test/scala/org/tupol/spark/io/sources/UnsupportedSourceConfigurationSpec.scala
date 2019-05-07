package org.tupol.spark.io.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Failure

class UnsupportedSourceConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration without schema") {
    val configStr = """format="unknown""""
    val config = ConfigFactory.parseString(configStr)

    val converterConfig = SourceConfiguration(config)

    converterConfig shouldBe a[Failure[_]]
  }
}
