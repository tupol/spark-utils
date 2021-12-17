package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.sql.loadSchemaFromFile

import scala.util.Failure

class ExtendedSchemaExtractorSpec extends AnyFunSuite with Matchers {

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json").get

  test("Load schema from an external resource with a schema configuration path") {
    val config = ConfigFactory.parseString(""" schema.path: "sources/avro/sample_schema.json" """)
    println(config.extract[StructType]("schema"))
    config.extract[StructType]("schema").get shouldBe ReferenceSchema
  }

  test("Load schema from an external resource without a schema configuration path") {
    val config = ConfigFactory.parseString("""path: "sources/avro/sample_schema.json" """)
    config.extract[StructType].get shouldBe ReferenceSchema
  }

  test("Load schema from a classpath resource with a schema configuration path") {
    val config = ConfigFactory.parseString(""" schema.path: "/sources/avro/sample_schema.json" """)
    config.extract[StructType]("schema").get shouldBe ReferenceSchema
  }

  test("Load schema from a classpath resource without a schema configuration path") {
    val config = ConfigFactory.parseString(""" path: "/sources/avro/sample_schema.json" """)
    config.extract[StructType].get shouldBe ReferenceSchema
  }

  test("Load schema from config with a schema configuration path") {
    val config = ConfigFactory.parseString(s"schema ${ReferenceSchema.prettyJson} ".stripMargin)
    config.extract[StructType]("schema").get shouldBe ReferenceSchema
  }

  test("Load schema from config without a schema configuration path") {
    val config = ConfigFactory.parseString(s"${ReferenceSchema.prettyJson} ".stripMargin)
    config.extract[StructType].get shouldBe ReferenceSchema
  }

  test("Fail to load schema from config without a schema configuration path") {
    val config = ConfigFactory.parseString(s"""{ INVALID_SCHEMA: "" } """.stripMargin)
    config.extract[StructType] shouldBe a [Failure[_]]
  }

  test("Fail to load schema from a classpath resource") {
    val config = ConfigFactory.parseString(""" schema.path: "this path does not actually exist" """)
    config.extract[StructType]("schema") shouldBe a [Failure[_]]
    println(config.extract[StructType]("schema"))
  }

}
