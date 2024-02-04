package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.config.ConfigOps
import org.tupol.spark.io.FormatType.Kafka
import org.tupol.spark.io.streaming.structured.GenericStreamDataSourceConfiguration

import scala.util.Failure

class GenericStreamDataSourceConfigurationSpec extends AnyFunSuite with Matchers {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

  test("Successfully extract a minimal GenericStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |format=kafka
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSourceConfiguration(Kafka)
    config.extract[GenericStreamDataSourceConfiguration].get shouldBe expected
  }

  test("Successfully extract a minimal GenericStreamDataSourceConfiguration out of a configuration string with options") {

    val configStr =
      """
        |format=kafka
        |options {
        | key1: val1
        | key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSourceConfiguration(Kafka, Map("key1" -> "val1", "key2" -> "val2"), None)
    config.extract[GenericStreamDataSourceConfiguration].get shouldBe expected
  }

  test("Failed to extract GenericStreamDataSourceConfiguration out of an empty string") {

    val configStr = ""
    val config    = ConfigFactory.parseString(configStr)

    config.extract[GenericStreamDataSourceConfiguration] shouldBe a[Failure[_]]
  }
}
