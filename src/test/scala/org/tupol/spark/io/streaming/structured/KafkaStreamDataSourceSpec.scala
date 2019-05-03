package org.tupol.spark.io.streaming.structured

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.spark.sql.streaming.Trigger
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Span }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._

class KafkaStreamDataSourceSpec extends FunSuite
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SharedSparkSession with EmbeddedKafka {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  implicit val config = EmbeddedKafkaConfig()
  val topic = "testTopic"

  val TestConfig = KafkaStreamDataSourceConfiguration(s":${config.kafkaPort}", KafkaSubscription("subscribe", topic), Some("earliest"))

  test("String messages should be written to the kafka stream and read back") {

    import spark.implicits._

    withRunningKafka {

      val data = spark.source(TestConfig).read

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
    val inputConfig = KafkaStreamDataSourceConfiguration(
      "unknown_host:0000000",
      KafkaSubscription("subscribe", topic), Some("earliest"))
    an[Exception] shouldBe thrownBy(spark.source(inputConfig).read)
  }

}
