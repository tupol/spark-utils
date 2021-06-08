package org.tupol.spark.io.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType.{ Json, Kafka, Socket, Text }
import org.tupol.spark.sql.loadSchemaFromFile
import org.tupol.utils.configz._

class FormatAwareStreamingSinkConfigurationSpec extends FunSuite with Matchers with SharedSparkSession {

  val ReferenceSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json")

  test("Successfully extract a Text FileStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="text"
        |input.options {}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSinkConfiguration(
      path = "INPUT_PATH",
      genericConfig = GenericStreamDataSinkConfiguration(Text, Map()))
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract a Json FileStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="json"
        |input.options {}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSinkConfiguration(
      path = "INPUT_PATH",
      genericConfig = GenericStreamDataSinkConfiguration(Json, Map()))
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract KafkaStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="kafka"
        |input.kafka.bootstrap.servers="test_server"
        |input.options {}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSinkConfiguration(
      kafkaBootstrapServers = "test_server",
      genericConfig = GenericStreamDataSinkConfiguration(Kafka, Map("kafka.bootstrap.servers" -> "test_server")))
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("input")

    result.get.asInstanceOf[KafkaStreamDataSinkConfiguration].generic shouldBe expected.generic
  }

  test("Successfully extract GenericStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="socket"
        |input.options {
        |   key1: val1
        |   key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Socket, Map("key1" -> "val1", "key2" -> "val2"))
    val result = config.extract[FormatAwareStreamingSinkConfiguration]("input")

    result.get shouldBe expected
  }

  test("Failed to extract FormatAwareStreamingSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="unknown"
        |input.options {
        |   key1: val1
        |   key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[FormatAwareStreamingSinkConfiguration]("input")

    noException shouldBe thrownBy(result.get)
  }

}
