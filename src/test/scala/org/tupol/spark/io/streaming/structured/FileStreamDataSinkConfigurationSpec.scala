package org.tupol.spark.io.streaming.structured

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.FormatType._
import org.tupol.utils.configz._

class FileStreamDataSinkConfigurationSpec extends FunSuite with Matchers {

  test("Successfully extract a minimal FileStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |format=json
        |path=my_path
        |options {
        |   key1: val1
        |   key2: val2
        |}
        |trigger: {
        |   type="continuous"
        |   interval="12 seconds"
        |}
        |queryName=testQueryName
        |partition.columns=["col1", "col2"]
        |outputMode=testOutputMode
        |checkpointLocation=myCheckpointLocation
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(
      Json,
      Map("key1" -> "val1", "key2" -> "val2", "path" -> "my_path", "checkpointLocation" -> "myCheckpointLocation"),
      Some("testQueryName"), Some(Trigger.Continuous(12000)), Seq("col1", "col2"), Some("testOutputMode"))
    config.extract[FileStreamDataSinkConfiguration].get.generic shouldBe expected
  }

  test("Successfully extract a minimal FileStreamDataSinkConfiguration out of a configuration string with empty options") {

    val configStr =
      """
        |format=csv
        |path=my_path
        |options {
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Csv, Map("path" -> "my_path"))
    config.extract[FileStreamDataSinkConfiguration].get.generic shouldBe expected
  }

  test("Successfully extract a minimal FileStreamDataSinkConfiguration out of a configuration string with options") {

    val configStr =
      """
        |format=parquet
        |path=my_path
        |options {
        | key1: val1
        | key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Parquet, Map("key1" -> "val1", "key2" -> "val2", "path" -> "my_path"))
    config.extract[FileStreamDataSinkConfiguration].get.generic shouldBe expected
  }

  test("Failed to extract FileStreamDataSinkConfiguration out of a configuration string if the format is unsupported") {

    val configStr =
      """
        |format=unsupported
        |path=my_path
        |options {
        | key1: val1
        | key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[FileStreamDataSinkConfiguration].get)
  }

  test("Failed to extract FileStreamDataSinkConfiguration out of a configuration string if options are missing") {

    val configStr =
      """
        |format=text
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[FileStreamDataSinkConfiguration].get)
  }
}
