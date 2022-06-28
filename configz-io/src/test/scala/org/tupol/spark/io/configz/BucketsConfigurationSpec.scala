package org.tupol.spark.io.configz

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.configz._
import org.tupol.spark.io.BucketsConfiguration

class BucketsConfigurationSpec extends AnyFunSuite with Matchers {

  implicit val BucketsConfigurationExtractor = BucketsConfigurator

  test("Successfully extract a full BucketsConfiguration") {

    val configStr =
      """
        |number=1
        |columns=["a", "b", "c"]
        |sortByColumns=["a", "b" ]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = BucketsConfiguration(
      number = 1,
      columns = Seq("a", "b", "c"),
      sortByColumns = Seq("a", "b"))

    config.extract[BucketsConfiguration].get shouldBe expected
  }

  test("Successfully extract a partial BucketsConfiguration") {

    val configStr =
      """
        |number=1
        |columns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = BucketsConfiguration(
      number = 1,
      columns = Seq("a", "b", "c"),
      sortByColumns = Seq())

    config.extract[BucketsConfiguration].get shouldBe expected
  }

  test("Failed BucketsConfiguration, missing columns") {

    val configStr =
      """
        |number=1
        |sortByColumns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[BucketsConfiguration].get)
  }

  test("Failed BucketsConfiguration, empty columns") {

    val configStr =
      """
        |number=1
        |sortByColumns=["a", "b", "c"]
        |columns=[]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[BucketsConfiguration].get)
  }

  test("Failed BucketsConfiguration, number = 0") {

    val configStr =
      """
        |number=0
        |columns=["a", "b", "c"]
        |sortByColumns=["a", "b" ]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[BucketsConfiguration].get)
  }

  test("Failed BucketsConfiguration, number < 0") {

    val configStr =
      """
        |number=-1
        |columns=["a", "b", "c"]
        |sortByColumns=["a", "b" ]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[BucketsConfiguration].get)
  }
}
