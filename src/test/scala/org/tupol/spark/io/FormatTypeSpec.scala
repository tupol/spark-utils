package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Failure

class FormatTypeSpec extends FunSuite with Matchers {

  import FormatType._

  test("fromString works on known types") {
    fromString("com.databricks.spark.avro").get shouldBe Avro
    fromString("avro").get shouldBe Avro
    fromString("com.databricks.spark.xml").get shouldBe Xml
    fromString("xml").get shouldBe Xml
    fromString("parquet").get shouldBe Parquet
    fromString("text").get shouldBe Text
    fromString("csv").get shouldBe Csv
    fromString("orc").get shouldBe Orc
  }

  test("fromString returns a failure for unknown types") {
    fromString("unknown_format_type") shouldBe a[Failure[_]]
  }

  test("FormatTypeExtractor - unknown") {
    import org.tupol.utils.config._
    val configStr = """ format=" unknown " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format")
    result shouldBe a[scalaz.Failure[_]]
  }

  test("FormatTypeExtractor - avro") {
    import org.tupol.utils.config._
    val configStr = """ format=" com.databricks.spark.avro " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Avro
  }

  test("FormatTypeExtractor - avro compact") {
    import org.tupol.utils.config._
    val configStr = """ format="avro " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Avro
  }

  test("FormatTypeExtractor - xml") {
    import org.tupol.utils.config._
    val configStr = """ format=" com.databricks.spark.xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Xml
  }

  test("FormatTypeExtractor - xml compact") {
    import org.tupol.utils.config._
    val configStr = """ format=" xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Xml
  }

}
