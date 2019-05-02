package org.tupol.spark.io.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.sources.TextSourceConfiguration

class FileStreamDataSourceConfigurationSpec extends FunSuite with Matchers {

  test("Successfully extract FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = TextSourceConfiguration())
    val result = FileStreamDataSourceConfiguration(config.getConfig("input"))

    result.get shouldBe expected
  }

  test("Failed to extract FileStreamDataSourceConfiguration if the format is not supported") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="jdbc"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileStreamDataSourceConfiguration(config.getConfig("input"))

    result.isSuccess shouldBe false

  }

  test("Failed to extract FileStreamDataSourceConfiguration if the path is not defined") {

    val configStr =
      """
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileStreamDataSourceConfiguration(config.getConfig("input"))

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileStreamDataSourceConfiguration if the format is not defined") {

    val configStr =
      """
        |input.path="INPUT_PATH"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileStreamDataSourceConfiguration(config.getConfig("input"))

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileStreamDataSourceConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = FileStreamDataSourceConfiguration(config)

    result.isSuccess shouldBe false
  }

}
