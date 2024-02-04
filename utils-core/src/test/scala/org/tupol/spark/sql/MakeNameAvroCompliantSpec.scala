package org.tupol.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.{ Failure, Success, Try }

class MakeNameAvroCompliantSpec extends AnyFunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  val acceptableChars: Seq[Char]  = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') :+ '_'
  val acceptableString            = acceptableChars.mkString("")
  val someNonCompliantAsciiString = (0 to 127).map(_.toChar).filterNot(acceptableChars.contains).mkString("")

  test("makeNameAvroCompliant removes the non-compliant chars") {
    val input    = acceptableString ++ someNonCompliantAsciiString
    val expected = acceptableString
    makeNameAvroCompliant(input, "", "", "") shouldBe expected
  }

  test("makeNameAvroCompliant replaces the first non-compliant char") {
    val input    = "05" + acceptableString
    val expected = "5" + acceptableString
    makeNameAvroCompliant(input, "", "", "") shouldBe expected
  }

  test("makeNameAvroCompliant does not replace the first non-compliant char if a prefix is specified") {
    val input    = "0" + acceptableString
    val expected = "_0" + acceptableString
    makeNameAvroCompliant(input, "", "_", "") shouldBe expected
  }

  test("makeNameAvroCompliant does not work with empty strings") {
    an[IllegalArgumentException] shouldBe thrownBy(makeNameAvroCompliant("", "", "", ""))
  }

  test("makeNameAvroCompliant reports non-compliant prefix") {
    an[IllegalArgumentException] shouldBe thrownBy(makeNameAvroCompliant(acceptableString, "_", "123", ""))
  }

  test("makeNameAvroCompliant reports non-compliant replaceWith") {
    an[IllegalArgumentException] shouldBe thrownBy(makeNameAvroCompliant(acceptableString, " ", "_", ""))
  }

  test("makeNameAvroCompliant reports non-compliant replaceWith first char if prefix is not specified") {
    an[IllegalArgumentException] shouldBe thrownBy(makeNameAvroCompliant(acceptableString, "0_", "", ""))
  }

  test("makeNameAvroCompliant works with non-compliant replaceWith first char if prefix is specified") {
    makeNameAvroCompliant(acceptableString, "0_", "_", "")
  }

  test("makeNameAvroCompliant reports non-compliant suffix") {
    an[IllegalArgumentException] shouldBe thrownBy(makeNameAvroCompliant(acceptableString, "_", "_", " "))
  }

  test("makeNameAvroCompliant successfully cleans the schema") {

    import org.tupol.spark.implicits._

    val inputData =
      spark.createDataFrame(Seq(BadAvroTest0(1, 2, 3, 4), BadAvroTest0(2, 3, 4, 5, Some(BadAvroTest1(1, 2, 3, 4, 5)))))
    val expectedData = spark.createDataFrame(
      Seq(GoodAvroTest0(1, 2, 3, 4), GoodAvroTest0(2, 3, 4, 5, Some(GoodAvroTest1(1, 2, 3, 4, 5))))
    )

    // Writing the original DataFrame to Avro should fail due to the non-compliant names
    Try(inputData.write.format("avro").save(testPath1)) shouldBe a[Failure[_]]

    val result = inputData.makeAvroCompliant

    // The data contents of the transformed DataFrame should be the same
    result.collect() should contain theSameElementsAs expectedData.collect()

    // Writing the "friendly" DataFrame to Avro should be successful
    Try(result.write.format("avro").save(testPath1)) shouldBe a[Success[_]]
  }

}

case class BadAvroTest0(
  `good_name`: Int,
  `_0_bad_name`: Int,
  `bad.name`: Int,
  `another@bad$name`: Int,
  `more%bad+names`: Option[BadAvroTest1] = None
)
case class BadAvroTest1(
  `good_name_123`: Int,
  `_01_bad_name`: Int,
  `bad name`: Int,
  `another#bad&name`: Int,
  `more^bad*names`: Int
)

case class GoodAvroTest0(
  `good_name`: Int,
  `__bad_name`: Int,
  `bad.name`: Int,
  `another@bad$name`: Int,
  `more%bad+names`: Option[GoodAvroTest1] = None
)
case class GoodAvroTest1(
  `good_name_123`: Int,
  `_1_bad_name`: Int,
  `bad_name`: Int,
  `another_bad_name`: Int,
  `more_bad_names`: Int
)
