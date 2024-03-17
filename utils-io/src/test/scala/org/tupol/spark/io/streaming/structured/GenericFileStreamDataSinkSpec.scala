package org.tupol.spark.io.streaming.structured

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.implicits._
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.{ TestTempFilePath1, TestTempFilePath2 }

import scala.util.Success

class GenericFileStreamDataSinkSpec
    extends AnyFunSuite
    with Matchers
    with Eventually
    with SharedSparkSession
    with TestTempFilePath1
    with TestTempFilePath2 {

  import spark.implicits._

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val TestData = Seq(TestRecord("v1", 1, 1.1, true), TestRecord("v2", 2, 2.2, false))

  test("Saving the input data as Json results in the same data") {
    val inputStream = MemoryStream[TestRecord]
    val data        = inputStream.toDF()
    inputStream.addData(TestData)

    val writeOptions = Map[String, String]("path" -> testPath1, "checkpointLocation" -> testPath2)
    val sinkConfig = GenericStreamDataSinkConfiguration(
      FormatType.Json,
      writeOptions,
      Some("testQuery"),
      Some(Trigger.ProcessingTime("1 second"))
    )

    val steamingQuery = data.streamingSink(sinkConfig).write
    steamingQuery shouldBe a[Success[_]]

    eventually {
      val sourceData             = spark.createDataFrame(TestData)
      val writtenData: DataFrame = spark.read.json(testPath1)
      writtenData.compareWith(sourceData).areEqual(false) shouldBe true
    }
    steamingQuery.get.stop()
  }

  test("Saving the input data as Parquet results in the same data") {
    val inputStream = MemoryStream[TestRecord]
    val data        = inputStream.toDF()
    inputStream.addData(TestData)

    val writeOptions = Map[String, String]("path" -> testPath1, "checkpointLocation" -> testPath2)
    val sinkConfig = GenericStreamDataSinkConfiguration(
      FormatType.Parquet,
      writeOptions,
      Some("testQuery"),
      Some(Trigger.ProcessingTime("1 second"))
    )

    val steamingQuery = data.streamingSink(sinkConfig).write
    steamingQuery shouldBe a[Success[_]]

    eventually {
      val sourceData             = spark.createDataFrame(TestData)
      val writtenData: DataFrame = spark.read.parquet(testPath1)
      writtenData.compareWith(sourceData).areEqual(false) shouldBe true
    }
    steamingQuery.get.stop()
  }

}
