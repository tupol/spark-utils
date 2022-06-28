package org.tupol.spark.io.configz

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.configz._
import org.tupol.spark.io.{FormatType, GenericSinkConfiguration}

class GenericSinkConfigurationSpec extends AnyFunSuite with Matchers {

  test("Successfully extract GenericSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="text"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericSinkConfiguration(
      format = FormatType.Text,
      mode = Some("MODE"),
      partitionColumns = Seq("OUTPUT_PATH"))

    val result = config.extract[GenericSinkConfiguration]("output")

    result.get shouldBe expected
  }

  test("Successfully extract GenericSinkConfiguration out of a configuration string without the partition files") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="custom.format"
        |output.mode="MODE"
        |output.partition.columns=["PARTITION"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericSinkConfiguration(
      format = FormatType.Custom("custom.format"),
      mode = Some("MODE"),
      partitionColumns = Seq("PARTITION"))

    val result = GenericSinkConfigurator.extract(config.getConfig("output"))

    result.get shouldBe expected
  }

  test("Successfully create GenericSinkConfiguration using the simplified constructor") {

    val result = GenericSinkConfiguration(FormatType.Text, Some("append"))

    val expected = GenericSinkConfiguration(
      format = FormatType.Text,
      mode = Some("append"),
      partitionColumns = Seq())

    result shouldBe expected
    result.saveMode shouldBe "append"
  }

  test("Successfully extract GenericSinkConfiguration even for a known format") {

    val configStr =
      """
        |output.format="avro"
        |output.mode="MODE"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[GenericSinkConfiguration]("output")

    result.isSuccess shouldBe true
  }

  test("Failed to extract GenericSinkConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = GenericSinkConfigurator.extract(config)

    result.isSuccess shouldBe false
  }

}
