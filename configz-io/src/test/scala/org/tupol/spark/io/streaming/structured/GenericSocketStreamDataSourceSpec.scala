package org.tupol.spark.io.streaming.structured

import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{ BeforeAndAfter, GivenWhenThen }
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Span }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.FormatType
import org.tupol.spark.testing.StringSocketSpec

class GenericSocketStreamDataSourceSpec extends AnyFunSuite
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SharedSparkSession with StringSocketSpec {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  val TestOptions = Map(
    "host" -> s"$host",
    "port" -> s"$port")

  val TestConfig = GenericStreamDataSourceConfiguration(FormatType.Socket, TestOptions, None)

  test("String messages should be written to the socket stream and read back") {

    import spark.implicits._

    val data = spark.source(TestConfig).read.get
      .withColumn("timestamp", current_timestamp())

    val streamingQuery = data.writeStream
      .format("memory")
      .queryName("result")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    val result = spark.table("result")

    val testMessages = (1 to 4).map(i => f"test-message-$i%02d")

    testMessages.foreach { message =>
      publishStringMessageToSocket(message)
      eventually {
        val received = result.select("value", "timestamp").as[(String, Long)]
          .collect().sortBy(_._2).reverse.headOption.map(_._1)
        received shouldBe Some(message)
      }
    }
    streamingQuery.stop
  }

}
