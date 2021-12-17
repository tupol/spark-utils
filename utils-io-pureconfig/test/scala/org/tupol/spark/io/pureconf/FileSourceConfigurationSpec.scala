package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.FileSourceConfiguration
import org.tupol.spark.io.sources.TextSourceConfiguration

class FileSourceConfigurationSpec extends AnyFunSuite with Matchers {

  import pureconfig.generic.auto._

  test("Successfully extract FileSourceConfiguration out of a configuration string") {

//    val configStr =
//      """
//        |input.path="INPUT_PATH"
//        |input.format="text"
//      """.stripMargin
//    val config = ConfigFactory.parseString(configStr)
//
//    println(config.extract[FileSourceConfiguration])
//
//    val expected = FileSourceConfiguration(
//      path = "INPUT_PATH",
//      sourceConfiguration = TextSourceConfiguration())
//    val result = FileSourceConfigurator.extract(config.getConfig("input"))
//
//    result.get shouldBe expected
  }

}
