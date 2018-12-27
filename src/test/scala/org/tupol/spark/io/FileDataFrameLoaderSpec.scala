package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.parsers.AvroConfiguration
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.Failure

class FileDataFrameLoaderSpec extends FunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  test("Loading the data fails if the file does not exist") {

    val inputPath = "unknown/path/to/inexistent/file.no.way"
    val parserOptions = Map[String, String]()
    val parserConfig = AvroConfiguration(parserOptions, None)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val resultDF1 = FileDataFrameLoader(inputConfig).loadData

    resultDF1 shouldBe a[Failure[_]]

    val resultDF2 = spark.loadData(inputConfig)
    resultDF2 shouldBe a[Failure[_]]
  }

}
