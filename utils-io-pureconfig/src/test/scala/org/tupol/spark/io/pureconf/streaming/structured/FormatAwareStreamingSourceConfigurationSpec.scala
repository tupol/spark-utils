package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf.config.ConfigOps
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType.Kafka
import org.tupol.spark.io.sources.{JsonSourceConfiguration, TextSourceConfiguration}
import org.tupol.spark.io.streaming.structured._
import org.tupol.spark.sql.loadSchemaFromFile

class FormatAwareStreamingSourceConfigurationSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json")

  test("Successfully extract text FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = TextSourceConfiguration())
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract json FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.path="INPUT_PATH"
        |input.format="json"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = FileStreamDataSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = JsonSourceConfiguration())
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract kafka FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="kafka"
        |input.kafkaBootstrapServers="my_server"
        |input.subscription.type="subscribePattern"
        |input.subscription.value="topic_*"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSourceConfiguration(
      kafkaBootstrapServers = "my_server",
      subscription = KafkaSubscription("subscribePattern", "topic_*"))
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Successfully extract generic FileStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format=kafka
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSourceConfiguration(Kafka)
    val result = config.extract[FormatAwareStreamingSourceConfiguration]("input")

    result.get shouldBe expected
  }

}
