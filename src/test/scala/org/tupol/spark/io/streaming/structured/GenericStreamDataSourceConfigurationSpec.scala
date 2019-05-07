package org.tupol.spark.io.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.FormatType.Kafka
import org.tupol.utils.config._

class GenericStreamDataSourceConfigurationSpec extends FunSuite with Matchers {

  test("Successfully extract a minimal GenericStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |format=kafka
        |options {}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSourceConfiguration(Kafka, Map())
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

    val expected = GenericStreamDataSourceConfiguration(Kafka, Map("key1" -> "val1", "key2" -> "val2"))
    config.extract[GenericStreamDataSourceConfiguration].get shouldBe expected
  }

  test("Failed to extract GenericStreamDataSourceConfiguration out of an empty string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[GenericStreamDataSourceConfiguration].get)
  }
}
