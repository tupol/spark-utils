package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FormatTypeSpec extends AnyFunSuite with Matchers {

  import FormatType._

  test("fromString works on known types") {
    fromString("com.databricks.spark.avro").get shouldBe Avro
    fromString("com.databricks.spark.xml").get shouldBe Xml
    fromString("xml").get shouldBe Xml
    fromString("parquet").get shouldBe Parquet
    fromString("text").get shouldBe Text
    fromString("csv").get shouldBe Csv
    fromString("orc").get shouldBe Orc
  }

  test("fromString returns a Custom format type") {
    fromString("unknown_format_type").get shouldBe FormatType.Custom("unknown_format_type")
  }

}
