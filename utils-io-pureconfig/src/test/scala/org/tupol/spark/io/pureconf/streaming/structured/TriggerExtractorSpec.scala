package org.tupol.spark.io.pureconf.streaming.structured

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf._

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class TriggerExtractorSpec extends AnyFunSuite with Matchers {

  import org.tupol.spark.io.pureconf.streaming.structured.readers._

  test("TriggerExtractor -> Trigger.Once()") {
    val configStr =
      """
        |trigger.type="once"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger]("trigger") shouldBe Success(Trigger.Once)
  }

  test("TriggerExtractor -> Trigger.Continuous() Failure") {
    val configStr =
      """
        |trigger.type="continuous"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger] shouldBe a [Failure[_]]

  }

  test("TriggerExtractor -> Trigger.Continuous()") {
    val configStr =
      """
        |trigger.type="continuous"
        |trigger.interval="12 seconds"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger]("trigger") shouldBe Success(Trigger.Continuous(12.seconds))
  }

  test("TriggerExtractor -> Trigger.ProcessingTime() Failure") {
    val configStr =
      """
        |trigger.type="ProcessingTime"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger]("trigger") shouldBe a [Failure[_]]
  }

  test("TriggerExtractor -> Trigger.ProcessingTime()") {
    val configStr =
      """
        |trigger.type=processing-time
        |trigger.interval="12 seconds"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger]("trigger") shouldBe Success(Trigger.ProcessingTime(12.seconds))
  }

  test("TriggerExtractor Fails on unsupported trigger type") {
    val configStr =
      """trigger.type="UNKNOWN"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger]("trigger") shouldBe a [Failure[_]]
  }

  test("TriggerExtractor Fails on empty") {
    val configStr =
      """
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[Trigger] shouldBe a [Failure[_]]
  }

}
