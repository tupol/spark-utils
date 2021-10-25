package org.tupol.spark.io.configz.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.configz._
import org.tupol.spark.io.configz._
import org.tupol.spark.io.streaming.structured.{KafkaStreamDataSourceConfiguration, KafkaSubscription}

class KafkaStreamDataSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Successfully extract a minimal KafkaStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |kafka.bootstrap.servers="server"
        |subscription.type="subscribePattern"
        |subscription.value="topic_*"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = KafkaStreamDataSourceConfiguration(
      kafkaBootstrapServers = "server",
      subscription = KafkaSubscription("subscribePattern", "topic_*"))
    val result = config.extract[KafkaStreamDataSourceConfiguration]

    result.get shouldBe expected
  }

  test("Successfully extract KafkaStreamDataSourceConfiguration out of a configuration string") {

    val configStr =
      """
        |input.format="kafka"
        |input.kafka.bootstrap.servers="server"
        |input.subscription.type="subscribe"
        |input.subscription.value="topic"
        |input.startingOffsets="earliest"
        |input.endingOffsets="latest"
        |input.failOnDataLoss="true"
        |input.kafkaConsumer.pollTimeoutMs=512
        |input.fetchOffset.numRetries=12
        |input.fetchOffset.retryIntervalMs=100
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
      schema = None)
    val result = config.extract[KafkaStreamDataSourceConfiguration]("input")

    result.get shouldBe expected
  }

  test("Failed to extract KafkaStreamDataSourceConfiguration if 'kafka.bootstrap.servers' is not defined") {

    val configStr =
      """
        |input.format="kafka"
        |input.subscription.type="subscribe"
        |input.subscription.value="topic"
        |input.startingOffsets="earliest"
        |input.endingOffsets="latest"
        |input.failOnDataLoss="true"
        |input.kafkaConsumer.pollTimeoutMs=512
        |input.fetchOffset.numRetries=12
        |input.fetchOffset.retryIntervalMs=100
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
        |input.kafka.bootstrap.servers="server"
        |input.subscription.type="UNKNOWN"
        |input.subscription.value="topic"
        |input.startingOffsets="earliest"
        |input.endingOffsets="latest"
        |input.failOnDataLoss="true"
        |input.kafkaConsumer.pollTimeoutMs=512
        |input.fetchOffset.numRetries=12
        |input.fetchOffset.retryIntervalMs=100
        |input.maxOffsetsPerTrigger=111
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.extract[KafkaStreamDataSourceConfiguration]("output")

    result.isSuccess shouldBe false
  }

  test("Failed to extract KafkaStreamDataSourceConfiguration out of an empty configuration string") {

    val configStr = ""
    val config = ConfigFactory.parseString(configStr)

    val result = KafkaStreamDataSourceConfigurator.extract(config)

    result.isSuccess shouldBe false
  }

}
