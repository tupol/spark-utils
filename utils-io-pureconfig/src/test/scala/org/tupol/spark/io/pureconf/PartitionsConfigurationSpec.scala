package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.PartitionsConfiguration

import scala.util.Failure

class PartitionsConfigurationSpec extends AnyFunSuite with Matchers {

  import org.tupol.spark.io.pureconf.readers._

  test("Successfully extract a full PartitionsConfiguration") {

    val configStr =
      """
        |number=1
        |columns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = PartitionsConfiguration(number = Some(1), columns = Seq("a", "b", "c"))

    config.extract[PartitionsConfiguration].get shouldBe expected
  }

  test("Successfully extract a partial PartitionsConfiguration") {

    val configStr =
      """
        |columns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = PartitionsConfiguration(number = None, columns = Seq("a", "b", "c"))

    config.extract[PartitionsConfiguration].get shouldBe expected
  }

  test("Failed PartitionsConfiguration, missing columns") {

    val configStr =
      """
        |number=1
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[PartitionsConfiguration] shouldBe a[Failure[_]]
  }

  test("Failed PartitionsConfiguration, number = 0") {

    val configStr =
      """
        |number=0
        |columns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[PartitionsConfiguration] shouldBe a[Failure[_]]
  }

  test("Failed PartitionsConfiguration, number < 0") {

    val configStr =
      """
        |number=-1
        |columns=["a", "b", "c"]
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    config.extract[PartitionsConfiguration] shouldBe a[Failure[_]]
  }
}
