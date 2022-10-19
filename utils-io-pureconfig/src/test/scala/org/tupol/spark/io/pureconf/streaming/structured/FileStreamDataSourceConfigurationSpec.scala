package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.config.ConfigOps
import org.tupol.spark.io.sources.TextSourceConfiguration
import org.tupol.spark.io.streaming.structured.FileStreamDataSourceConfiguration

class FileStreamDataSourceConfigurationSpec extends AnyFunSuite with Matchers {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

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
    val result = config.extract[FileStreamDataSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Failed to extract FileStreamDataSourceConfiguration if the format is not supported") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="jdbc"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.getConfig("input").extract[FileStreamDataSourceConfiguration]

    result.isSuccess shouldBe false

  }

  test("Failed to extract FileStreamDataSourceConfiguration if the path is not defined") {

    val configStr =
      """
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.getConfig("input").extract[FileStreamDataSourceConfiguration]

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileStreamDataSourceConfiguration if the format is not defined") {

    val configStr =
      """
        |input.path="INPUT_PATH"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.getConfig("input").extract[FileStreamDataSourceConfiguration]

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileStreamDataSourceConfiguration if the format is incorrect") {

    val configStr =
      """
        |input.format="unknown"
        |input.path="INPUT_PATH"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.getConfig("input").extract[FileStreamDataSourceConfiguration]

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileStreamDataSourceConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[FileStreamDataSourceConfiguration]

    result.isSuccess shouldBe false
  }

}
