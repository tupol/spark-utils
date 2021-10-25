package org.tupol.spark.io.configz.streaming.structured

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfter, GivenWhenThen}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.configz._
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.streaming.structured.GenericStreamDataSourceConfiguration

class GenericKafkaStreamDataSourceSpec extends AnyFunSuite
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SharedSparkSession with EmbeddedKafka {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  implicit val config = EmbeddedKafkaConfig()
  val topic = "testTopic"

  val TestOptions = Map(
    "kafka.bootstrap.servers" -> s":${config.kafkaPort}",
    "subscribe" -> topic,
    "startingOffsets" -> "earliest")

  val TestConfig = GenericStreamDataSourceConfiguration(FormatType.Kafka, TestOptions, None)

  test("String messages should be written to the kafka stream and read back") {

    import spark.implicits._

    withRunningKafka {

      val data = spark.source(TestConfig).read.get

      val streamingQuery = data.writeStream
        .format("memory")
        .queryName("result")
        .trigger(Trigger.ProcessingTime(1000))
        .start()

      val result = spark.table("result")

      val testMessages = (1 to 4).map(i => f"test-message-$i%02d")

      testMessages.foreach { message =>
        publishStringMessageToKafka(topic, message)
        eventually {
          val received = result.select("value", "timestamp").as[(String, Long)]
            .collect().sortBy(_._2).reverse.headOption.map(_._1)
          received shouldBe Some(message)
        }
      }
      streamingQuery.stop
    }
  }

  test("Fail gracefully") {
    val TestOptions = Map(
      "kafka.bootstrap.servers" -> s"unknown_host_garbage_string:0000000",
      "NO-LEGAL-SUBSCRIPTION-TYPE" -> topic,
      "NO-STARTING-OFFSETS" -> "garbage")
    val inputConfig = GenericStreamDataSourceConfiguration(FormatType.Kafka, TestOptions, None)
    an[Exception] shouldBe thrownBy(spark.source(inputConfig).read.get)
  }

}
