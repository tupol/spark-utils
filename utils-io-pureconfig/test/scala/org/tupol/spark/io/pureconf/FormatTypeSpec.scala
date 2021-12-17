package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io._

class FormatTypeSpec extends AnyFunSuite with Matchers {

  test("FormatTypeExtractor - custom") {
    val configStr = """ format=" unknown " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Custom("unknown")
  }

  test("FormatTypeExtractor - avro") {
    val configStr = """ format=" com.databricks.spark.avro " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Avro
  }

  test("FormatTypeExtractor - xml") {
    val configStr = """ format=" com.databricks.spark.xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Xml
  }

  test("FormatTypeExtractor - xml compact") {
    val configStr = """ format=" xml " """
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FormatType]("format").get
    result shouldBe FormatType.Xml
  }

}
