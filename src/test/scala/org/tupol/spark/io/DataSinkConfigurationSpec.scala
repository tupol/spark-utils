package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.utils.config._

class DataSinkConfigurationSpec extends FunSuite with Matchers {

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

    val result = config.extract[DataSinkConfiguration]("output")

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
    val result = config.extract[DataSinkConfiguration]("output")

    result.get shouldBe expected
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
    val result = config.extract[DataSinkConfiguration]("output")

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
    val result = config.extract[DataSinkConfiguration]("output")

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSinkConfiguration if the format is not acceptable") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="UNKNOWN_FORMAT"
        |output.mode="overwrite"
        |output.partition.columns=["OUTPUT_PATH"]
        |output.partition.files=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[DataSinkConfiguration]("output")

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
    val result = config.extract[DataSinkConfiguration]("output")

    result.isSuccess shouldBe false
  }

  test("Failed to extract FileSinkConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[DataSinkConfiguration]

    result.isSuccess shouldBe false
  }

  test("Successfully extract JdbcSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.format="jdbc"
        |output.url="OUTPUT_URL"
        |output.table="SOURCE_TABLE"
        |output.user="USER_NAME"
        |output.password="USER_PASS"
        |output.driver="SOME_DRIVER"
        |output.mode="SOME_MODE"
        |output.options={
        |  opt1: "val1"
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = JdbcSinkConfiguration(
      url = "OUTPUT_URL",
      table = "SOURCE_TABLE",
      user = Some("USER_NAME"),
      password = Some("USER_PASS"),
      driver = Some("SOME_DRIVER"),
      optionalSaveMode = Some("SOME_MODE"),
      options = Map("opt1" -> "val1"))
    val result = config.extract[DataSinkConfiguration]("output")

    result.get shouldBe expected
  }

  test("Failed to extract JdbcSinkConfiguration if the url is not defined") {

    val configStr =
      """
        |output.format="jdbc"
        |output.table="SOURCE_TABLE"
        |output.user="USER_NAME"
        |output.password="USER_PASS"
        |output.driver="SOME_DRIVER"
        |output.mode="SOME_MODE"
        |output.options={
        |  opt1: "val1"
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[DataSinkConfiguration]("output")

    result.isSuccess shouldBe false
  }

}
