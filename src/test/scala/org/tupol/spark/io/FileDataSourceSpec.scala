package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.AvroSourceConfiguration

class FileDataSourceSpec extends FunSuite with Matchers with SharedSparkSession {

  test("Loading the data fails if the file does not exist") {

    val inputPath = "unknown/path/to/inexistent/file.no.way"
    val options = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(options, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)

    a[DataSourceException] should be thrownBy FileDataSource(inputConfig).read

    a[DataSourceException] should be thrownBy spark.source(inputConfig).read
  }
}
