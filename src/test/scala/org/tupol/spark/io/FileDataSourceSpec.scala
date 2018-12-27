package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.AvroSourceConfiguration

import scala.util.Failure

class FileDataSourceSpec extends FunSuite with Matchers with SharedSparkSession {

  test("Loading the data fails if the file does not exist") {

    val inputPath = "unknown/path/to/inexistent/file.no.way"
    val options = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(options, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF1 = FileDataSource(inputConfig).read

    resultDF1 shouldBe a[Failure[_]]

    val resultDF2 = spark.source(inputConfig).read
    resultDF2 shouldBe a[Failure[_]]
  }
}
