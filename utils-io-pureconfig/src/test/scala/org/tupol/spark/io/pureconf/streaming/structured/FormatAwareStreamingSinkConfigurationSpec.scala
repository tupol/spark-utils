package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.pureconf.config.ConfigOps
import org.tupol.spark.io.FormatType.{Json, Kafka, Socket, Text}
import org.tupol.spark.io.streaming.structured.{FileStreamDataSinkConfiguration, FormatAwareStreamingSinkConfiguration, GenericStreamDataSinkConfiguration, KafkaStreamDataSinkConfiguration}
import org.tupol.spark.sql.loadSchemaFromFile

import scala.util.Failure

class FormatAwareStreamingSinkConfigurationSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json")

  test("Successfully extract a Text FileStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSinkConfiguration(
      path = "OUTPUT_PATH",
      genericConfig = GenericStreamDataSinkConfiguration(Text, Map()),
      checkpointLocation = None
    ).resolve
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("output")

    result.get.asInstanceOf[FileStreamDataSinkConfiguration].resolve shouldBe expected
  }

  test("Successfully extract a Json FileStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.path="OUTPUT_PATH"
        |output.format="json"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSinkConfiguration(
      path = "OUTPUT_PATH",
      genericConfig = GenericStreamDataSinkConfiguration(Json, Map()),
      checkpointLocation = None).resolve
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("output")

    result.get.asInstanceOf[FileStreamDataSinkConfiguration].resolve shouldBe expected
  }

  test("Successfully extract KafkaStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.format="kafka"
        |output.kafkaBootstrapServers="test_server"
        |output.options {
        |   key1: val1
        |   key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSinkConfiguration(
      kafkaBootstrapServers = "test_server",
      genericConfig = GenericStreamDataSinkConfiguration(Kafka, Map("kafka.bootstrap.servers" -> "test_server", "key1" -> "val1", "key2" -> "val2")))
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("output")

    result.get.asInstanceOf[KafkaStreamDataSinkConfiguration].generic shouldBe expected.generic
  }

  test("Successfully extract GenericStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.format="socket"
        |output.options {
        |   key1: val1
        |   key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Socket, Map("key1" -> "val1", "key2" -> "val2"))
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("output")

    result.get shouldBe expected
  }

  test("Failed to extract FormatAwareStreamingSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.format="unknown"
        |output.options {
        |   key1: val1
        |   key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[FormatAwareStreamingSinkConfiguration]("output")

    result shouldBe a[Failure[_]]
  }

}
