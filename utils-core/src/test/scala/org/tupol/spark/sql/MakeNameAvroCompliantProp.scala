package org.tupol.spark.sql

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.{ const, _ }
import org.scalacheck.Prop.forAll
import org.scalacheck.{ Gen, Properties }
import org.scalatest.matchers.should.Matchers

import scala.util.{ Failure, Try }

class MakeNameAvroCompliantProp extends Properties("makeNameAvroCompliant") with Matchers {

  def acceptableFirstChar(char: Char): Boolean =
    (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '_'
  def acceptableTailChar(char: Char): Boolean = char.isDigit || acceptableFirstChar(char)
  def acceptableName(name: String): Boolean =
    name.filterNot(acceptableTailChar).size == 0 && acceptableFirstChar(name.head)

  val avroFiendlyNames: Gen[String] = for {
    start <- Gen.oneOf(alphaChar, const[Char]('_'))
    end   <- Gen.listOf(Gen.frequency((9, alphaNumChar), (1, const('_'))))
  } yield start.toString + end.mkString

  val avroNegativeFirstCharDigit: Gen[String] = for {
    name <- arbitrary[String] suchThat (s => s.size > 1 && s.head.isDigit && acceptableName(s.tail))
  } yield name

  val avroNegativeNames: Gen[String] = for {
    name <- arbitrary[String] suchThat (s => !s.isEmpty && !acceptableName(s))
  } yield name

  property("makeNameAvroCompliant should not change the name if the name is Avro compliant") =
    forAll(avroFiendlyNames) { (name: String) =>
      makeNameAvroCompliant(name, "xyz", "", "") === name
    }

  property("makeNameAvroCompliant should change the name if the name is not Avro compliant") =
    forAll(avroNegativeNames) { (name: String) =>
      name.trim match {
        case "" => Try(makeNameAvroCompliant(name, "xyz", "", "")).isInstanceOf[Failure[_]]
        case _  => makeNameAvroCompliant(name, "xyz", "", "") != name
      }
    }

  property(
    "makeNameAvroCompliant should produce shorter names if the name is not Avro compliant and replaceWith, prefix and suffix are empty"
  ) = forAll(avroNegativeNames) { (name: String) =>
    name.trim match {
      case "" => Try(makeNameAvroCompliant(name, "", "", "")).isInstanceOf[Failure[_]]
      case _  => makeNameAvroCompliant(name, "", "", "").size < name.size
    }
  }

  property(
    "makeNameAvroCompliant should produce same size names if the name is not Avro compliant and replaceWith is a single char, prefix and suffix are empty"
  ) = forAll(avroNegativeNames) { (name: String) =>
    name.trim match {
      case "" => Try(makeNameAvroCompliant(name, "_", "", "")).isInstanceOf[Failure[_]]
      case _  => makeNameAvroCompliant(name, "_", "", "").size == name.size
    }
  }

  property("makeNameAvroCompliant should prepend the specified prefix if the name is not Avro compliant") =
    forAll(avroNegativeNames) { (name: String) =>
      name.trim match {
        case "" => Try(makeNameAvroCompliant(name, "", "", "")).isInstanceOf[Failure[_]]
        case _  => makeNameAvroCompliant(name, "", "TEST_PREFIX_", "").startsWith("TEST_PREFIX_")
      }
    }

  property("makeNameAvroCompliant should append the specified suffix if the name is not Avro compliant") =
    forAll(avroNegativeNames) { (name: String) =>
      name.trim match {
        case "" => Try(makeNameAvroCompliant(name, "", "", "")).isInstanceOf[Failure[_]]
        case _  => makeNameAvroCompliant(name, "", "", "_TEST_SUFFIX").endsWith("_TEST_SUFFIX")
      }
    }

}
