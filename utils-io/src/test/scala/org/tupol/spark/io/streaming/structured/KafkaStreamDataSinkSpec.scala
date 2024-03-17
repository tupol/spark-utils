package org.tupol.spark.io.streaming.structured

import io.github.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.DataSinkException
import org.tupol.spark.io.FormatType.Kafka
import org.tupol.spark.io.implicits._
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.Success

class KafkaStreamDataSinkSpec
    extends AnyFunSuite
    with Matchers
    with Eventually
    with SharedSparkSession
    with TestTempFilePath1
    with EmbeddedKafka {

  import spark.implicits._

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  override val sparkConfig = super.sparkConfig + ("spark.io.compression.codec" -> "snappy")

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
  val topic                                = "testTopic"

  test("Saving the input data as Json results in the same data") {
    val TestData      = Seq(TestRecord("v1", 1, 1.1, true), TestRecord("v2", 2, 2.2, false))
    val TestDataFrame = spark.createDataFrame(TestData)

    val inputStream = MemoryStream[TestRecord]
    val data        = inputStream.toDF().toJSON.toDF("value")
    inputStream.addData(TestData)

    val genericConfig =
      GenericStreamDataSinkConfiguration(Kafka, Map(), Some("testQuery"), Some(Trigger.ProcessingTime("1 second")))
    val sinkConfig =
      KafkaStreamDataSinkConfiguration(s":${config.kafkaPort}", genericConfig, Some(topic), Some(testPath1))

    withRunningKafka {
      val steamingQuery = data.streamingSink(sinkConfig).write

      steamingQuery shouldBe a[Success[_]]
      eventually {
        val writtenData = consumeNumberStringMessagesFrom(topic, 2)
        val writtenDataFrame =
          spark.read.json(spark.createDataFrame(writtenData.map(x => (x, x))).as[(String, String)].map(_._1))
        writtenDataFrame.compareWith(TestDataFrame).areEqual(false) shouldBe true
      }
      steamingQuery.get.stop()
    }
  }

  test("Fail gracefully") {
    val TestData = Seq(TestRecord("v1", 1, 1.1, true), TestRecord("v2", 2, 2.2, false))

    val inputStream = MemoryStream[TestRecord]
    val data        = inputStream.toDF().toJSON.toDF("value")
    inputStream.addData(TestData)

    val genericConfig =
      GenericStreamDataSinkConfiguration(Kafka, Map(), Some("testQuery"), Some(Trigger.ProcessingTime("1 second")))
    val sinkConfig = KafkaStreamDataSinkConfiguration("unknown_host:0000000", genericConfig)

    withRunningKafka {
      a[DataSinkException] shouldBe thrownBy(data.streamingSink(sinkConfig).write.get)
    }
  }

}
