package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io._

class FormatTypeSpec extends AnyFunSuite with Matchers {

  import pureconfig.generic.auto._
  import org.tupol.spark.io.pureconf.readers.FormatTypeReader

  test("FormatTypeExtractor - custom") {
    val configStr = """ format = unknown  """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[TestFormatType].get
    result.format shouldBe FormatType.Custom("unknown")
  }

  test("FormatTypeExtractor - avro") {
    val configStr = """ format=" com.databricks.spark.avro " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[TestFormatType].get
    result.format shouldBe FormatType.Avro
  }

  test("FormatTypeExtractor - xml") {
    val configStr = """ format=" com.databricks.spark.xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[TestFormatType].get
    result.format shouldBe FormatType.Xml
  }

  test("FormatTypeExtractor - xml compact") {
    val configStr = """ format=" xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[TestFormatType].get
    result.format shouldBe FormatType.Xml
  }

}

case class TestFormatType(format: FormatType)