package org.tupol.spark.app

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.utils.fuzzyLoadTextResourceFile

import scala.util.Failure

class UtilsSpec extends AnyFunSuite with Matchers {

  val expectedText = Seq("line 1", "line 2").mkString("\n")

  test("fuzzyLoadTextResourceFile fails while trying to load an empty path") {
    val path = ""
    fuzzyLoadTextResourceFile("") shouldBe a[Failure[_]]
  }

  test("fuzzyLoadTextResourceFile fails while trying to load a path that does not exist anywhere") {
    val path = "/unknown/path/leading/to/unknown/file/s1n.ci7y"
    fuzzyLoadTextResourceFile(path) shouldBe a[Failure[_]]
  }

  test("fuzzyLoadTextResourceFile successfully loads a text from a local path") {
    val path   = "src/test/resources/utils/sample-text.resource"
    val result = fuzzyLoadTextResourceFile(path).get
    result shouldBe expectedText
  }

  test("fuzzyLoadTextResourceFile successfully loads a text from the class path") {
    val path   = "utils/sample-text.resource"
    val result = fuzzyLoadTextResourceFile(path).get
    result shouldBe expectedText
  }

  test("fuzzyLoadTextResourceFile successfully loads a text from the URI") {
    val path   = new java.io.File("src/test/resources/utils/sample-text.resource").toURI.toASCIIString
    val result = fuzzyLoadTextResourceFile(path).get
    result shouldBe expectedText
  }

  test("fuzzyLoadTextResourceFile successfully loads a text from the URL") {
    val path   = "http://info.cern.ch"
    val result = fuzzyLoadTextResourceFile(path).get
    // If this test fails it's probably because one of the oldest websites retreated into darkness. May that never happen :)
    result.size should be > 10
  }

}
