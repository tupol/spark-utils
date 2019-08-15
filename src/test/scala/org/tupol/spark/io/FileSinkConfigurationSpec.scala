package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.utils.config._

class FileSinkConfigurationSpec extends FunSuite with Matchers {

  test("Successfully extract FileSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="text"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileSinkConfiguration(
      path = "OUTPUT_PATH",
      format = FormatType.Text,
      optionalSaveMode = Some("MODE"),
      partitionColumns = Seq("OUTPUT_PATH"),
      partitionFilesNumber = Some(2))

    val result = config.extract[FileSinkConfiguration]("output")

    result.get shouldBe expected
  }

  test("Successfully extract FileSinkConfiguration out of a configuration string without the partition files") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="text"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileSinkConfiguration(
      path = "OUTPUT_PATH",
      format = FormatType.Text,
      optionalSaveMode = Some("MODE"),
      partitionColumns = Seq("OUTPUT_PATH"),
      partitionFilesNumber = None)

    val result = FileSinkConfiguration(config.getConfig("output"))

    result.get shouldBe expected
  }

  test("Successfully create FileSinkConfiguration using the simplified constructor") {

    val result = FileSinkConfiguration("OUTPUT_PATH", FormatType.Text)

    val expected = FileSinkConfiguration(
      path = "OUTPUT_PATH",
      format = FormatType.Text,
      optionalSaveMode = None,
      partitionColumns = Seq(),
      partitionFilesNumber = None)

    result shouldBe expected
    result.saveMode shouldBe "default"
    result.toString.contains("OUTPUT_PATH") shouldBe true
  }

  test("Failed to extract FileSinkConfiguration if the path is not defined") {

    val configStr =
      """
        |output.format="avro"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[FileSinkConfiguration]("output")

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSinkConfiguration if the format is not defined") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[FileSinkConfiguration]("output")

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSinkConfiguration if the format is not acceptable") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="custom.format"
        |output.mode="overwrite"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileSinkConfiguration(config.getConfig("output"))

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSinkConfiguration if the partition.files is a number smaller than 0") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="parquet"
        |output.mode="append"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=-2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileSinkConfiguration(config.getConfig("output"))

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSinkConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = FileSinkConfiguration(config)

    result.isSuccess shouldBe false
  }

}
