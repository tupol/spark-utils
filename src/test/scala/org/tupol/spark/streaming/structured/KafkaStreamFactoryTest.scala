package org.tupol.spark.streaming.structured

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.spark.sql.streaming.Trigger
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Span }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType

class KafkaStreamFactoryTest extends FlatSpec
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SharedSparkSession with EmbeddedKafka {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  implicit val config = EmbeddedKafkaConfig()
  val topic = "testTopic"

  val TestOptions = Map(
    "kafka.bootstrap.servers" -> s":${config.kafkaPort}",
    "subscribe" -> topic,
    "startingOffsets" -> "earliest")

  val TestConfig = StreamDataSourceConfiguration(FormatType.Kafka, TestOptions, None)

  "String messages" should "be written to the kafka stream, transformed and read back" in {

    import spark.implicits._

    withRunningKafka {

      val data = StreamDataSource(TestConfig).read

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

}
