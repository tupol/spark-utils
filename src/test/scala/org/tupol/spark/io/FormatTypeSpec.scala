package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

class FormatTypeSpec extends FunSuite with Matchers {

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

  test("FormatTypeExtractor - custom") {
    import org.tupol.utils.configz._
    val configStr = """ format=" unknown " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Custom("unknown")
  }

  test("FormatTypeExtractor - avro") {
    import org.tupol.utils.configz._
    val configStr = """ format=" com.databricks.spark.avro " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Avro
  }

  test("FormatTypeExtractor - xml") {
    import org.tupol.utils.configz._
    val configStr = """ format=" com.databricks.spark.xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Xml
  }

  test("FormatTypeExtractor - xml compact") {
    import org.tupol.utils.configz._
    val configStr = """ format=" xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Xml
  }

}
