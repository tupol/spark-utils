package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.FileSourceConfiguration
import org.tupol.spark.io.sources.{CsvSourceConfiguration, TextSourceConfiguration}
import org.tupol.spark.io.pureconf.readers._

class FileSourceConfigurationSpec extends AnyFunSuite with Matchers {

  test("Successfully extract a text FileSourceConfiguration out of a configuration string") {

    val expected = FileSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = TextSourceConfiguration())

    val configStr =
      """
        |path="INPUT_PATH"
        |format="text"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val result = config.extract[FileSourceConfiguration]

    result.get shouldBe expected
  }

  test("Successfully extract a csv FileSourceConfiguration out of a configuration string") {

    val expected = FileSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = CsvSourceConfiguration(Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8",
        "header" -> "true", "delimiter" -> "DELIMITER")))

    val configStr =
      """
        |format="csv"
        |path="INPUT_PATH"
        |header=true
        |delimiter="DELIMITER"
        |options {
        |   mode : "PERMISSIVE"
        |   samplingRatio : "1"
        |   charset : "UTF-8"
        |   delimiter="CONFUSING_INNER_DELIMITER_THAT_WILL_BE_OVERWRITTEN"
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = FileSourceConfigurator.extract(config)

    result.get shouldBe expected
  }

}
