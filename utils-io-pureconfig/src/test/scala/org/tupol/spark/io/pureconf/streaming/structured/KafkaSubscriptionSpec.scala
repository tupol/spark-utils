package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf._
import org.tupol.spark.io.streaming.structured.KafkaSubscription

import scala.util.{ Failure, Success }

class KafkaSubscriptionSpec extends AnyFunSuite with Matchers {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

  test("KafkaSubscription.assign") {
    val configStr =
      """
        |type="assign"
        |value="some_value"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription] shouldBe Success(KafkaSubscription("assign", "some_value"))
  }

  test("KafkaSubscription.subscribe") {
    val configStr =
      """
        |type="subscribe"
        |value="some_value"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription] shouldBe Success(KafkaSubscription("subscribe", "some_value"))
  }

  test("KafkaSubscription.subscribePattern") {
    val configStr =
      """
        |type="subscribePattern"
        |value="some_value"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription] shouldBe Success(KafkaSubscription("subscribePattern", "some_value"))
  }

  test("KafkaSubscription Fails on unsupported type") {
    val configStr =
      """|type="UNKNOWN"
         |value="some_value"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription] shouldBe a[Failure[_]]
  }

  test("KafkaSubscription Fails on empty") {
    val configStr =
      """
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[KafkaSubscription] shouldBe a[Failure[_]]
  }

}
