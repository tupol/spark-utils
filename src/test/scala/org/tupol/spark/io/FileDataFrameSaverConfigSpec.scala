package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

class FileDataFrameSaverConfigSpec extends FunSuite with Matchers {

  test("Successfully extract FileDataFrameSaverConfig out of a configuration string") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="text"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileDataFrameSaverConfig(
      path = "OUTPUT_PATH",
      format = FormatType.Text,
      optionalSaveMode = Some("MODE"),
      partitionColumns = Seq("OUTPUT_PATH"),
      partitionFilesNumber = Some(2))

    val result = FileDataFrameSaverConfig(config.getConfig("output"))

    result.isSuccess shouldBe true
    result.get shouldBe expected
    result.get.saveMode shouldBe "MODE"
    result.get.toString.contains("path") shouldBe true
  }

  test("Extract FileDataFrameSaverConfig out of a configuration string when the partition.files is smaller than 1 yields default partitioning") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="parquet"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=0
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileDataFrameSaverConfig(
      path = "OUTPUT_PATH",
      format = FormatType.Parquet,
      optionalSaveMode = Some("MODE"),
      partitionColumns = Seq("OUTPUT_PATH"),
      partitionFilesNumber = None)

    val result = FileDataFrameSaverConfig(config.getConfig("output"))

    result.isSuccess shouldBe true
    result.get shouldBe expected
  }

  test("Successfully create FileDataFrameSaverConfig using the simplified constructor") {

    val result = FileDataFrameSaverConfig("OUTPUT_PATH", FormatType.Text)

    val expected = FileDataFrameSaverConfig(
      path = "OUTPUT_PATH",
      format = FormatType.Text,
      optionalSaveMode = None,
      partitionColumns = Seq(),
      partitionFilesNumber = None)

    result shouldBe expected
    result.saveMode shouldBe "default"
    result.toString.contains("OUTPUT_PATH") shouldBe true
  }

  test("Failed to extract FileDataFrameSaverConfig if the path is not defined") {

    val configStr =
      """
        |output.format="FORMAT"
        |mode="MODE"
        |partition.columns=["OUTPUT_PATH"]
        |partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileDataFrameSaverConfig(config)

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileDataFrameSaverConfig if the format is not defined") {

    val configStr =
      """
        |path="OUTPUT_PATH"
        |mode="MODE"
        |partition.columns=["OUTPUT_PATH"]
        |partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileDataFrameSaverConfig(config)

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileDataFrameSaverConfig if the format is not correctly") {

    val configStr =
      """
        |path="OUTPUT_PATH"
        |output.format="UNKNOWN_FORMAT"
        |mode="MODE"
        |partition.columns=["OUTPUT_PATH"]
        |partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileDataFrameSaverConfig(config)

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileDataFrameSaverConfig out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = FileDataFrameSaverConfig(config)

    result.isSuccess shouldBe false
  }

}
