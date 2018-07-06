package io.tupol.spark.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import scalaz.syntax.applicative._

import scala.collection.JavaConversions._
import scala.util.{ Success, Try }

class ExtractorSpec extends FunSuite with Matchers {

  case class ComplexExample(char: Character, str: String, bool: Boolean, dbl: Double, in: Int, lng: Long,
    optChar: Option[Character], optStr: Option[String], optBool: Option[Boolean],
    optDouble: Option[Double], optInt: Option[Int], optLong: Option[Long])

  test("Missing optional values should not result in error") {
    val complexInstance = ComplexExample('*', "string", false, 0.9d, 23, 12L, None, None, None, None, None, None)
    val config = ConfigFactory.parseMap(Map("char" -> "*", "str" -> "string", "bool" -> "false", "dbl" -> "0.9", "in" -> "23", "lng" -> "12"))
    val validationResult: Try[ComplexExample] =
      config.extract[Character]("char") |@| config.extract[String]("str") |@|
        config.extract[Boolean]("bool") |@| config.extract[Double]("dbl") |@|
        config.extract[Int]("in") |@| config.extract[Long]("lng") |@|
        config.extract[Option[Character]]("optchar") |@| config.extract[Option[String]]("optstr") |@|
        config.extract[Option[Boolean]]("optbool") |@| config.extract[Option[Double]]("optdbl") |@|
        config.extract[Option[Int]]("optin") |@| config.extract[Option[Long]]("optlng") apply ComplexExample.apply

    validationResult shouldEqual Success(complexInstance)
  }

  test("Optional values should be parsed when present") {
    val complexInstance = ComplexExample('*', "string", false, 0.9d, 23, 12L, Some('*'), Some("string"), Some(false), Some(0.9d), Some(23), Some(12l))
    val config = ConfigFactory.parseMap(Map("char" -> "*", "str" -> "string", "bool" -> "false", "dbl" -> "0.9", "in" -> "23", "lng" -> "12",
      "optchar" -> "*", "optstr" -> "string", "optbool" -> "false", "optdbl" -> "0.9", "optin" -> "23", "optlng" -> "12"))
    val validationResult: Try[ComplexExample] = config.extract[Character]("char") |@| config.extract[String]("str") |@| config.extract[Boolean]("bool") |@| config.extract[Double]("dbl") |@|
      config.extract[Int]("in") |@| config.extract[Long]("lng") |@| config.extract[Option[Character]]("optchar") |@| config.extract[Option[String]]("optstr") |@|
      config.extract[Option[Boolean]]("optbool") |@| config.extract[Option[Double]]("optdbl") |@| config.extract[Option[Int]]("optin") |@| config.extract[Option[Long]]("optlng") apply ComplexExample.apply

    validationResult shouldEqual Success(complexInstance)
  }

  case class SimpleExample(char: Character, bool: Boolean, in: Int)

  test("Missing non optional values should result in a Failure") {
    val config = ConfigFactory.parseMap(Map("char" -> "*", "in" -> "23"))
    val validationResult: Try[SimpleExample] = config.extract[Character]("char") |@| config.extract[Boolean]("bool") |@| config.extract[Int]("in") apply SimpleExample.apply

    validationResult.failed.map(_.getMessage).get should include("No configuration setting found for key 'bool'")
  }

  test("Non optional values of the wrong type should result in a Failure") {
    val config = ConfigFactory.parseMap(Map("char" -> "*", "bool" -> "nee", "in" -> "234,34"))
    val validationResult: Try[SimpleExample] = config.extract[Character]("char") |@| config.extract[Boolean]("bool") |@| config.extract[Int]("in") apply SimpleExample.apply
    validationResult.failed.map(_.getMessage).get should include("hardcoded value: bool has type STRING rather than BOOLEAN")
    validationResult.failed.map(_.getMessage).get should include("hardcoded value: in has type STRING rather than NUMBER")
  }

  test("Map[String, String] should be parsed from an object") {
    val config = ConfigFactory.parseString("""
      |property {
      |   key1 : 600
      |   key2 : "100"
      |}
    """.stripMargin)
    val actual: Map[String, String] = config.extract[Map[String, String]]("property").get
    actual shouldEqual Map("key1" -> "600", "key2" -> "100")
  }

  test("Map[String, String] should be parsed from an empty object") {
    val config = ConfigFactory.parseString("""
      |property {}
    """.stripMargin)
    val actual: Map[String, String] = config.extract[Map[String, String]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, String] should be parsed from a flat object") {
    val config = ConfigFactory.parseString("""
      |property.key1 = value1
      |property.key2 = value2
    """.stripMargin)
    val actual: Map[String, String] = config.extract[Map[String, String]]("property").get
    actual shouldEqual Map("key1" -> "value1", "key2" -> "value2")
  }

  test("Map[String, String] should be parsed from a list of entries") {
    val config = ConfigFactory.parseString("""
      |property :[
      |   {key1 : 600},
      |   {key2 : "100"}
      |]
    """.stripMargin)
    val actual: Map[String, String] = config.extract[Map[String, String]]("property").get
    actual shouldEqual Map("key1" -> "600", "key2" -> "100")
  }

  test("Map[String, String] should be parsed from an empty list of entries") {
    val config = ConfigFactory.parseString("""
      |property :[]
    """.stripMargin)
    val actual: Map[String, String] = config.extract[Map[String, String]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Any] should be parsed from an object") {
    val config = ConfigFactory.parseString("""
      |property {
      |   key1 : 600
      |   key2 : "100"
      |}
    """.stripMargin)
    val actual: Map[String, Any] = config.extract[Map[String, Any]]("property").get
    actual shouldEqual Map("key1" -> 600, "key2" -> "100")
  }

  test("Map[String, Any] should be parsed from an empty object") {
    val config = ConfigFactory.parseString("""
      |property {}
    """.stripMargin)
    val actual: Map[String, Any] = config.extract[Map[String, Any]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Any] should be parsed from a flat object") {
    val config = ConfigFactory.parseString("""
      |property.key1 = value1
      |property.key2 = value2
    """.stripMargin)
    val actual: Map[String, Any] = config.extract[Map[String, Any]]("property").get
    actual shouldEqual Map("key1" -> "value1", "key2" -> "value2")
  }

  test("Map[String, Any] should be parsed from a list of entries") {
    val config = ConfigFactory.parseString("""
      |property :[
      |   {key1 : 600},
      |   {key2 : "100"}
      |]
    """.stripMargin)
    val actual: Map[String, Any] = config.extract[Map[String, Any]]("property").get
    actual shouldEqual Map("key1" -> 600, "key2" -> "100")
  }

  test("Map[String, Any] should be parsed from an empty list of entries") {
    val config = ConfigFactory.parseString("""
      |property :[]
    """.stripMargin)
    val actual: Map[String, Any] = config.extract[Map[String, Any]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Double] should be parsed from an object") {
    val config = ConfigFactory.parseString("""
                                             |property {
                                             |   key1 : 1.1
                                             |   key2 : "2.2"
                                             |}
                                           """.stripMargin)
    val actual: Map[String, Double] = config.extract[Map[String, Double]]("property").get
    actual shouldEqual Map("key1" -> 1.1, "key2" -> 2.2)
  }

  test("Map[String, Double] should be parsed from an empty object") {
    val config = ConfigFactory.parseString("""
                                             |property {}
                                           """.stripMargin)
    val actual: Map[String, Double] = config.extract[Map[String, Double]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Double] should be parsed from a flat object") {
    val config = ConfigFactory.parseString("""
                                             |property.key1 = 1.1
                                             |property.key2 = 2.2
                                           """.stripMargin)
    val actual: Map[String, Double] = config.extract[Map[String, Double]]("property").get
    actual shouldEqual Map("key1" -> 1.1, "key2" -> 2.2)
  }

  test("Map[String, Double] should be parsed from a list of entries") {
    val config = ConfigFactory.parseString("""
                                             |property :[
                                             |   {key1 : 1.1},
                                             |   {key2 : "2.2"}
                                             |]
                                           """.stripMargin)
    val actual: Map[String, Double] = config.extract[Map[String, Double]]("property").get
    actual shouldEqual Map("key1" -> 1.1, "key2" -> 2.2)
  }

  test("Map[String, Double] should be parsed from an empty list of entries") {
    val config = ConfigFactory.parseString("""
                                             |property :[]
                                           """.stripMargin)
    val actual: Map[String, Double] = config.extract[Map[String, Double]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Int] should be parsed from an object") {
    val config = ConfigFactory.parseString("""
                                             |property {
                                             |   key1 : 1
                                             |   key2 : "2"
                                             |}
                                           """.stripMargin)
    val actual: Map[String, Int] = config.extract[Map[String, Int]]("property").get
    actual shouldEqual Map("key1" -> 1, "key2" -> 2)
  }

  test("Map[String, Int] should be parsed from an empty object") {
    val config = ConfigFactory.parseString("""
                                             |property {}
                                           """.stripMargin)
    val actual: Map[String, Int] = config.extract[Map[String, Int]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Int] should be parsed from a flat object") {
    val config = ConfigFactory.parseString("""
                                             |property.key1 = 1
                                             |property.key2 = 2
                                           """.stripMargin)
    val actual: Map[String, Int] = config.extract[Map[String, Int]]("property").get
    actual shouldEqual Map("key1" -> 1, "key2" -> 2)
  }

  test("Map[String, Int] should be parsed from a list of entries") {
    val config = ConfigFactory.parseString("""
                                             |property :[
                                             |   {key1 : 1},
                                             |   {key2 : "2"}
                                             |]
                                           """.stripMargin)
    val actual: Map[String, Int] = config.extract[Map[String, Int]]("property").get
    actual shouldEqual Map("key1" -> 1, "key2" -> 2)
  }

  test("Map[String, Int] should be parsed from an empty list of entries") {
    val config = ConfigFactory.parseString("""
                                             |property :[]
                                           """.stripMargin)
    val actual: Map[String, Int] = config.extract[Map[String, Int]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Long] should be parsed from an object") {
    val config = ConfigFactory.parseString("""
                                             |property {
                                             |   key1 : 1
                                             |   key2 : "2"
                                             |}
                                           """.stripMargin)
    val actual: Map[String, Long] = config.extract[Map[String, Long]]("property").get
    actual shouldEqual Map("key1" -> 1, "key2" -> 2)
  }

  test("Map[String, Long] should be parsed from an empty object") {
    val config = ConfigFactory.parseString("""
                                             |property {}
                                           """.stripMargin)
    val actual: Map[String, Long] = config.extract[Map[String, Long]]("property").get
    actual shouldEqual Map()
  }

  test("Map[String, Long] should be parsed from a flat object") {
    val config = ConfigFactory.parseString("""
                                             |property.key1 = 1
                                             |property.key2 = 2
                                           """.stripMargin)
    val actual: Map[String, Long] = config.extract[Map[String, Long]]("property").get
    actual shouldEqual Map("key1" -> 1, "key2" -> 2)
  }

  test("Map[String, Long] should be parsed from a list of entries") {
    val config = ConfigFactory.parseString("""
                                             |property :[
                                             |   {key1 : 1},
                                             |   {key2 : "2"}
                                             |]
                                           """.stripMargin)
    val actual: Map[String, Long] = config.extract[Map[String, Long]]("property").get
    actual shouldEqual Map("key1" -> 1, "key2" -> 2)
  }

  test("Map[String, Long] should be parsed from an empty list of entries") {
    val config = ConfigFactory.parseString("""
                                             |property :[]
                                           """.stripMargin)
    val actual: Map[String, Long] = config.extract[Map[String, Long]]("property").get
    actual shouldEqual Map()
  }

}
