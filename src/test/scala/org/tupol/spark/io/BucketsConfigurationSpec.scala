package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.utils.config._

class BucketsConfigurationSpec extends FunSuite with Matchers {

  implicit val BucketsConfigurationExtractor = BucketsConfiguration

  test("Successfully extract a full BucketsConfiguration") {

    val configStr =
      """
        |number=1
        |bucketColumns=["a", "b", "c"]
        |sortByColumns=["a", "b" ]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = BucketsConfiguration(
      number = 1,
      bucketColumns = Seq("a", "b", "c"),
      sortByColumns = Seq("a", "b"))

    config.extract[BucketsConfiguration].get shouldBe expected
  }

  test("Successfully extract a partial BucketsConfiguration") {

    val configStr =
      """
        |number=1
        |bucketColumns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = BucketsConfiguration(
      number = 1,
      bucketColumns = Seq("a", "b", "c"),
      sortByColumns = Seq())

    config.extract[BucketsConfiguration].get shouldBe expected
  }

  test("Failed BucketsConfiguration, missing bucketColumns") {

    val configStr =
      """
        |number=1
        |sortByColumns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[BucketsConfiguration].get)
  }

  test("Failed BucketsConfiguration, number = 0") {

    val configStr =
      """
        |number=0
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[BucketsConfiguration].get)
  }
}
