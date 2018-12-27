package org.tupol.spark.io

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.StructType
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.utils.config._
import org.tupol.spark.sql.loadSchemaFromFile

class ExtendedSchemaExtractorSpec extends FunSuite with Matchers {

  val ReferenceSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json")

  test("Load schema from an external resource with a schema configuration path") {
    val config = ConfigFactory.parseString(""" schema.path: "src/test/resources/sources/avro/sample_schema.json" """)
    config.extract[StructType]("schema").get shouldBe ReferenceSchema
  }

  test("Load schema from an external resource without a schema configuration path") {
    val config = ConfigFactory.parseString("""path: "src/test/resources/sources/avro/sample_schema.json" """)
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

}
