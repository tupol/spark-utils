package org.tupol.spark.streaming.structured

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType
import org.tupol.spark.streaming.structured.implicits._
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.{ Success, Try }

class KafkaStreamDataSinkSpec extends FunSuite with Matchers with Eventually with SharedSparkSession
  with TestTempFilePath1 with EmbeddedKafka {

  import spark.implicits._

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  implicit val config = EmbeddedKafkaConfig()
  val topic = "testTopic"

  test("Saving the input data as Json results in the same data") {
    val TestData = Seq(
      TestRecord("v1", 1, 1.1, true),
      TestRecord("v2", 2, 2.2, false))
    val TestDataFrame = spark.createDataFrame(TestData)

    val inputStream = MemoryStream[TestRecord]
    val data = inputStream.toDF.toJSON.toDF("value")
    inputStream.addData(TestData)

    val TestOptions = Map(
      "kafka.bootstrap.servers" -> s":${config.kafkaPort}",
      "topic" -> topic,
      "checkpointLocation" -> testPath1)

    val sinkConfig = GenericStreamDataSinkConfiguration(FormatType.Kafka, TestOptions, Some("TestQuery"))

    withRunningKafka {
      val steamingQuery = Try(data.sink(sinkConfig).write)
      steamingQuery shouldBe a[Success[_]]
      eventually {
        val writtenData = consumeNumberStringMessagesFrom(topic, 2)
        val writtenDataFrame = spark.read.json(spark.createDataFrame(writtenData.map(x => (x, x))).as[(String, String)].map(_._1))
        writtenDataFrame.comapreWith(TestDataFrame).areEqual(false) shouldBe true
      }
      steamingQuery.get.stop
    }
  }

}
