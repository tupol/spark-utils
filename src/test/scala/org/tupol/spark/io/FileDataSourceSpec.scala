package org.tupol.spark.io

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.{ AvroSourceConfiguration, JsonSourceConfiguration }
import org.tupol.spark.sql.loadSchemaFromFile

class FileDataSourceSpec extends FunSuite with Matchers with SharedSparkSession {

  test("Loading the data fails if the file does not exist") {

    val inputPath = "unknown/path/to/inexistent/file.no.way"
    val options = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(options, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)

    a[DataSourceException] should be thrownBy FileDataSource(inputConfig).read

    a[DataSourceException] should be thrownBy spark.source(inputConfig).read
  }

  test("Loading a json data source works") {

    val schema = loadSchemaFromFile("src/test/resources/sources/json/sample_schema-2.json")
    val inputPath = "src/test/resources/sources/json/ManyFiles/*.json"
    val mode = "FAILFAST"
    val options = Map[String, String]("columnNameOfCorruptRecord" -> "_corrupt_record", "mode" -> mode)
    val parserConfig = JsonSourceConfiguration(options, Some(schema))
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val resultDF1 = FileDataSource(inputConfig).read

    resultDF1.schema.fields.map(_.name) should contain allElementsOf (schema.fields.map(_.name))
  }
}
