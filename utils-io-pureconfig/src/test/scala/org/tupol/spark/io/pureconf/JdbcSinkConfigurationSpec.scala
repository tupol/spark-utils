package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.JdbcSinkConfiguration

import scala.util.{ Failure, Success }

class JdbcSinkConfigurationSpec extends AnyFunSuite with Matchers {

  import pureconfig.generic.auto._

  test("Successfully extract JdbcSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |output.format="jdbc"
        |output.url="OUTPUT_URL"
        |output.table="SOURCE_TABLE"
        |output.user="USER_NAME"
        |output.password="USER_PASS"
        |output.driver="SOME_DRIVER"
        |output.mode="SOME_MODE"
        |output.options={
        |  opt1: "val1"
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = JdbcSinkConfiguration(
      url = "OUTPUT_URL",
      table = "SOURCE_TABLE",
      user = Some("USER_NAME"),
      password = Some("USER_PASS"),
      driver = Some("SOME_DRIVER"),
      mode = Some("SOME_MODE"),
      options = Map("opt1" -> "val1")
    )
    val result = config.getConfig("output").extract[JdbcSinkConfiguration]

    result shouldBe Success(expected)
  }

  test("Failed to extract JdbcSinkConfiguration if the url is not defined") {

    val configStr =
      """
        |output.format="jdbc"
        |output.table="SOURCE_TABLE"
        |output.user="USER_NAME"
        |output.password="USER_PASS"
        |output.driver="SOME_DRIVER"
        |output.mode="SOME_MODE"
        |output.options={
        |  opt1: "val1"
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val result = config.getConfig("output").extract[JdbcSinkConfiguration]("output")

    result shouldBe a[Failure[_]]
  }

  test("Failed to extract JdbcSinkConfiguration out of an empty configuration string") {

    val configStr = ""
    val config    = ConfigFactory.parseString(configStr)

    val result = config.extract[JdbcSinkConfiguration]

    result shouldBe a[Failure[_]]
  }

}
