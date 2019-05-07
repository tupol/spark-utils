package org.tupol.spark.io.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.utils.config._

class KafkaSubscriptionExtractorSpec extends FunSuite with Matchers {

  test("KafkaSubscriptionExtractor -> assign") {
    val configStr =
      """
        |subscription.type="assign"
        |subscription.value="topic"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription].get shouldBe KafkaSubscription("assign", "topic")
  }

  test("KafkaSubscriptionExtractor -> assign Failure") {
    val configStr =
      """
        |subscription.type="subscribe"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[KafkaSubscription].get)
  }

  test("KafkaSubscriptionExtractor -> subscribe") {
    val configStr =
      """
        |subscription.type="subscribe"
        |subscription.value="topic"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription].get shouldBe KafkaSubscription("subscribe", "topic")
  }

  test("KafkaSubscriptionExtractor -> KafkaSubscription.ProcessingTime() Failure") {
    val configStr =
      """
        |subscription.type="ProcessingTime"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[KafkaSubscription].get)
  }

  test("KafkaSubscriptionExtractor -> KafkaSubscription.ProcessingTime()") {
    val configStr =
      """
        |subscription.type="subscribePattern"
        |subscription.value="topic"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription].get shouldBe KafkaSubscription("subscribePattern", "topic")
  }

  test("KafkaSubscriptionExtractor Fails on unsupported subscription type") {
    val configStr =
      """subscription.type="UNKNOWN"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[KafkaSubscription].get)
  }

  test("KafkaSubscriptionExtractor Fails on empty") {
    val configStr =
      """
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[KafkaSubscription].get)
  }

}
