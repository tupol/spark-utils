package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.config.ConfigOps
import org.tupol.spark.io.FormatType.Kafka
import org.tupol.spark.io.streaming.structured.GenericStreamDataSinkConfiguration

import scala.util.Failure

class GenericStreamDataSinkConfigurationSpec extends AnyFunSuite with Matchers {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

  test("Successfully extract a minimal GenericStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |format=kafka
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
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(
      Kafka,
      Map("key1" -> "val1", "key2" -> "val2"),
      Some("testQueryName"),
      Some(Trigger.Continuous(12000)),
      Seq("col1", "col2"),
      Some("testOutputMode")
    )
    config.extract[GenericStreamDataSinkConfiguration].get shouldBe expected
  }

  test(
    "Successfully extract a minimal GenericStreamDataSinkConfiguration out of a configuration string with empty options"
  ) {

    val configStr =
      """
        |format=kafka
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Kafka)
    config.extract[GenericStreamDataSinkConfiguration].get shouldBe expected
  }

  test("Successfully extract a minimal GenericStreamDataSinkConfiguration out of a configuration string with options") {

    val configStr =
      """
        |format=kafka
        |options {
        | key1: val1
        | key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Kafka, Map("key1" -> "val1", "key2" -> "val2"))
    config.extract[GenericStreamDataSinkConfiguration].get shouldBe expected
  }

  test("Failed to extract GenericStreamDataSinkConfiguration out of an empty string") {

    val configStr = ""
    val config    = ConfigFactory.parseString(configStr)

    config.extract[GenericStreamDataSinkConfiguration] shouldBe a[Failure[_]]
  }
}
