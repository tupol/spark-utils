package org.tupol.spark.io.streaming.structured

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{ BeforeAndAfter, GivenWhenThen }
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Span }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.TextSourceConfiguration
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.Random

class FileStreamDataSourceSpec extends AnyFunSuite
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SharedSparkSession with TestTempFilePath1 {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  test("String messages should be written to the file stream and read back") {

    import spark.implicits._

    FileUtils.forceMkdir(testFile1)

    val options = Map[String, String]("path" -> testPath1)
    val parserConfig = TextSourceConfiguration(options, None)
    val inputConfig = FileStreamDataSourceConfiguration(testPath1, parserConfig)

    val data = spark.source(inputConfig).read.get
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

  test("Fail gracefully") {
    val options = Map[String, String]("path" -> testPath1)
    val parserConfig = TextSourceConfiguration(options, None)
    val inputConfig = FileStreamDataSourceConfiguration(testPath1, parserConfig)
    an[Exception] shouldBe thrownBy(spark.source(inputConfig).read.get)
  }

  def addFile(text: String, parentFile: File): Unit = {
    val file = new File(parentFile, f"test-${math.abs(Random.nextLong())}%010d")
    FileUtils.write(file, text)
  }

}
