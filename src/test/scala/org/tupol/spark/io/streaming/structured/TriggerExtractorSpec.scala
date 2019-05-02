package org.tupol.spark.io.streaming.structured

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.utils.config._

class TriggerExtractorSpec extends FunSuite with Matchers {

  test("TriggerExtractor -> Trigger.Once()") {
    val configStr =
      """
        |trigger.type="once"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger].get shouldBe Trigger.Once()
  }

  test("TriggerExtractor -> Trigger.Continuous() Failure") {
    val configStr =
      """
        |trigger.type="continuous"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[Trigger].get)
  }

  test("TriggerExtractor -> Trigger.Continuous()") {
    val configStr =
      """
        |trigger.type="continuous"
        |trigger.interval="12 seconds"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger].get shouldBe Trigger.Continuous(12000)
  }

  test("TriggerExtractor -> Trigger.ProcessingTime() Failure") {
    val configStr =
      """
        |trigger.type="ProcessingTime"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[Trigger].get)
  }

  test("TriggerExtractor -> Trigger.ProcessingTime()") {
    val configStr =
      """
        |trigger.type="ProcessingTime"
        |trigger.interval="12 seconds"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger].get shouldBe Trigger.ProcessingTime(12000)
  }

  test("TriggerExtractor Fails on unsupported trigger type") {
    val configStr =
      """trigger.type="UNKNOWN"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[Trigger].get)
  }

  test("TriggerExtractor Fails on empty") {
    val configStr =
      """
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[Trigger].get)
  }

}
