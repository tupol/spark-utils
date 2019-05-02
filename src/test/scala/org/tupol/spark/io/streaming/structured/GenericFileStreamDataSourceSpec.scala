package org.tupol.spark.io.structured

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.Trigger
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Span }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io._
import org.tupol.spark.io.streaming.structured.GenericStreamDataSourceConfiguration
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.Random

class GenericFileStreamDataSourceSpec extends FunSuite
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SharedSparkSession with TestTempFilePath1 {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  test("String messages should be written to the file stream and read back using a GenericStreamDataSource") {

    import spark.implicits._

    FileUtils.forceMkdir(testFile1)

    val options = Map[String, String]("path" -> testPath1)

    val inputConfig = GenericStreamDataSourceConfiguration(FormatType.Text, options, None)

    val data = spark.source(inputConfig).read
      .withColumn("timestamp", current_timestamp())

    val streamingQuery = data.writeStream
      .format("memory")
      .queryName("result")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    val result = spark.table("result")

    val testMessages = (1 to 4).map(i => f"test-message-$i%02d")

    testMessages.foreach { message =>
      addFile(message, testFile1)
      eventually {
        val received = result.select("value", "timestamp").as[(String, Long)]
          .collect().sortBy(_._2).reverse.headOption.map(_._1)
        received shouldBe Some(message)
      }
    }
    streamingQuery.stop
  }

  def addFile(text: String, parentFile: File): Unit = {
    val file = new File(parentFile, f"test-${math.abs(Random.nextLong())}%010d")
    FileUtils.write(file, text)
  }

}
