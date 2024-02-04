package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.FormatType.Kafka
import org.tupol.spark.io.pureconf.config.ConfigOps
import org.tupol.spark.io.streaming.structured.{
  GenericStreamDataSourceConfiguration,
  KafkaStreamDataSourceConfiguration,
  KafkaSubscription
}

import scala.util.Failure

class KafkaStreamDataSourceConfigurationSpec extends AnyFunSuite with Matchers {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

  test("Successfully extract a minimal KafkaStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |kafkaBootstrapServers="server"
        |subscription.type="subscribePattern"
        |subscription.value="topic_*"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSourceConfiguration(
      kafkaBootstrapServers = "server",
      subscription = KafkaSubscription("subscribePattern", "topic_*")
    )
    val result = config.extract[KafkaStreamDataSourceConfiguration]

    result.get shouldBe expected
  }

  test("Successfully extract KafkaStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="kafka"
        |input.kafkaBootstrapServers="server"
        |input.subscription.type="subscribe"
        |input.subscription.value="topic"
        |input.startingOffsets="earliest"
        |input.endingOffsets="latest"
        |input.failOnDataLoss="true"
        |input.kafkaConsumerPollTimeoutMs=512
        |input.fetchOffsetNumRetries=12
        |input.fetchOffsetRetryIntervalMs=100
        |input.maxOffsetsPerTrigger=111
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSourceConfiguration(
      kafkaBootstrapServers = "server",
      subscription = KafkaSubscription("subscribe", "topic"),
      startingOffsets = Some("earliest"),
      endingOffsets = Some("latest"),
      failOnDataLoss = Some(true),
      kafkaConsumerPollTimeoutMs = Some(512),
      fetchOffsetNumRetries = Some(12),
      fetchOffsetRetryIntervalMs = Some(100),
      maxOffsetsPerTrigger = Some(111),
      schema = None
    )
    val result = config.extract[KafkaStreamDataSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("KafkaStreamDataSourceConfiguration required params overwrite the extra options when overlapping") {

    val configStr =
      """
        |input.format="kafka"
        |input.kafkaBootstrapServers="test_server"
        |input.subscription.type="subscribe"
        |input.subscription.value="my_topic"
        |input.startingOffsets="earliest"
        |input.endingOffsets="latest"
        |input.failOnDataLoss="true"
        |input.kafkaConsumerPollTimeoutMs=512
        |input.fetchOffsetNumRetries=12
        |input.fetchOffsetRetryIntervalMs=100
        |input.maxOffsetsPerTrigger=111
        |input.options {
        |  "kafka.bootstrap.servers": "other-server"
        |  "startingOffsets" : "random"
        |   key1: val1
        |   key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSourceConfiguration(
      kafkaBootstrapServers = "test_server",
      subscription = KafkaSubscription("subscribe", "my_topic"),
      startingOffsets = Some("earliest"),
      endingOffsets = Some("latest"),
      failOnDataLoss = Some(true),
      kafkaConsumerPollTimeoutMs = Some(512),
      fetchOffsetNumRetries = Some(12),
      fetchOffsetRetryIntervalMs = Some(100),
      maxOffsetsPerTrigger = Some(111),
      schema = None,
      options = Map(
        "kafka.bootstrap.servers" -> "other-server",
        "startingOffsets"         -> "random",
        "key1"                    -> "val1",
        "key2"                    -> "val2"
      )
    )
    val result = config.extract[KafkaStreamDataSourceConfiguration]("input")

    result.get shouldBe expected

    val expectedGeneric = GenericStreamDataSourceConfiguration(
      Kafka,
      Some(
        Map(
          "kafka.bootstrap.servers"     -> "test_server",
          "kafkaConsumer.pollTimeoutMs" -> "512",
          "subscribe"                   -> "my_topic",
          "fetchOffset.numRetries"      -> "12",
          "fetchOffset.retryIntervalMs" -> "100",
          "failOnDataLoss"              -> "true",
          "startingOffsets"             -> "earliest",
          "endingOffsets"               -> "latest",
          "maxOffsetsPerTrigger"        -> "111",
          "key1"                        -> "val1",
          "key2"                        -> "val2"
        )
      )
    )

    result.get.generic shouldBe expectedGeneric
  }

  test("Failed to extract KafkaStreamDataSourceConfiguration if 'kafkaBootstrapServers' is not defined") {

    val configStr =
      """
        |input.format="kafka"
        |input.subscription.type="subscribe"
        |input.subscription.value="topic"
        |input.startingOffsets="earliest"
        |input.endingOffsets="latest"
        |input.failOnDataLoss="true"
        |input.kafkaConsumerPollTimeoutMs=512
        |input.fetchOffsetNumRetries=12
        |input.fetchOffsetRetryIntervalMs=100
        |input.maxOffsetsPerTrigger=111
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[KafkaStreamDataSourceConfiguration]("output")

    result.isSuccess shouldBe false
  }

  test("Failed to extract KafkaStreamDataSourceConfiguration if 'subscription' is not defined") {

    val configStr =
      """
        |input.format="kafka"
        |input.kafkaBootstrapServers="server"
        |input.subscription.type="UNKNOWN"
        |input.subscription.value="topic"
        |input.startingOffsets="earliest"
        |input.endingOffsets="latest"
        |input.failOnDataLoss="true"
        |input.kafkaConsumerPollTimeoutMs=512
        |input.fetchOffsetNumRetries=12
        |input.fetchOffsetRetryIntervalMs=100
        |input.maxOffsetsPerTrigger=111
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[KafkaStreamDataSourceConfiguration]("output")

    result.isSuccess shouldBe false
  }

  test("Failed to extract KafkaStreamDataSourceConfiguration out of an empty configuration string") {

    val configStr = ""
    val config    = ConfigFactory.parseString(configStr)

    config.extract[KafkaStreamDataSourceConfiguration] shouldBe a[Failure[_]]
  }

}
