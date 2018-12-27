package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.sources.TextSourceConfiguration

class FileSourceConfigurationSpec extends FunSuite with Matchers {

  test("Successfully extract FileSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = TextSourceConfiguration())
    val result = FileSourceConfiguration(config.getConfig("input"))

    result.get shouldBe expected
  }

  test("Failed to extract FileSourceConfiguration if the format is not supported") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="jdbc"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileSourceConfiguration(config.getConfig("input"))

    result.isSuccess shouldBe false

  }

  test("Failed to extract FileSourceConfiguration if the path is not defined") {

    val configStr =
      """
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileSourceConfiguration(config.getConfig("input"))

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSourceConfiguration if the format is not defined") {

    val configStr =
      """
        |input.path="INPUT_PATH"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileSourceConfiguration(config.getConfig("input"))

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSourceConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = FileSourceConfiguration(config)

    result.isSuccess shouldBe false
  }

}
