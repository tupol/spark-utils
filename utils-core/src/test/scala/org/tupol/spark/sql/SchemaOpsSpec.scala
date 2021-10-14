package org.tupol.spark.sql

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.implicits._

class SchemaOpsSpec extends AnyFunSuite with Matchers {

  test("mapFields & checkAllFields") {

    val schema = StructType((Seq(
      StructField("col1", StringType, false),
      StructField("col2", StringType, false),
      StructField("col3", ArrayType(StructType(Array(
        StructField("col31", StringType, false),
        StructField("col32", StructType(Seq(
          StructField("col321", StructType(Seq(
            StructField("col3211", StringType, false),
            StructField("col3212", StringType, false),
            StructField("col3213", StringType, false))), false),
          StructField("col322", ArrayType(StructType(Array(
            StructField("col3221", StringType, false),
            StructField("col3222", StringType, false))), false)))), false)))), false),
      StructField("col4", MapType(
        StringType,
        StructType(Seq(StructField("col4v", StringType, false))), false), false))))

    val schemaNotNullFields = schema.mapFields((field: StructField) => field.copy(nullable = false))

    checkAllFields(schemaNotNullFields, (field: StructField) => field.nullable == true) shouldBe false

    schemaNotNullFields.checkAllFields((field: StructField) => field.nullable == true) shouldBe false

    val schemaNullFields = mapFields(schema, (field: StructField) => field.copy(nullable = true)).asInstanceOf[StructType]

    checkAllFields(schemaNullFields, (field: StructField) => field.nullable == true) shouldBe true

    schemaNullFields.checkAllFields((field: StructField) => field.nullable == true) shouldBe true
  }

  test("checkAllFields should fail inside array") {

    val schema = StructType((Seq(
      StructField("col1", StringType, false),
      StructField("col2", StringType, false),
      StructField("col3", ArrayType(StructType(Array(
        StructField("col31", StringType, false),
        StructField("col32", StructType(Seq(
          StructField("col321", StructType(Seq(
            StructField("col3211", StringType, false),
            StructField("col3212", StringType, false),
            StructField("col3213", StringType, true) // This one is true
          )), false),
          StructField("col322", ArrayType(StructType(Array(
            StructField("col3221", StringType, false),
            StructField("col3222", StringType, false))), false)))), false)))), false),
      StructField("col4", MapType(
        StringType,
        StructType(Seq(StructField("col4v", StringType, false))), false), false))))

    checkAllFields(schema, (field: StructField) => field.nullable == false) shouldBe false
    schema.checkAllFields((field: StructField) => field.nullable == false) shouldBe false
  }

  test("checkAllFields should fail inside map") {

    val schema = StructType((Seq(
      StructField("col1", StringType, false),
      StructField("col2", StringType, false),
      StructField("col3", ArrayType(StructType(Array(
        StructField("col31", StringType, false),
        StructField("col32", StructType(Seq(
          StructField("col321", StructType(Seq(
            StructField("col3211", StringType, false),
            StructField("col3212", StringType, false),
            StructField("col3213", StringType, false))), false),
          StructField("col322", ArrayType(StructType(Array(
            StructField("col3221", StringType, false),
            StructField("col3222", StringType, false))), false)))), false)))), false),
      StructField("col4", MapType(
        StringType,
        StructType(Seq(StructField("col4v", StringType, true))), false // This one is true
      ), false))))

    checkAllFields(schema, (field: StructField) => field.nullable == false) shouldBe false
    schema.checkAllFields((field: StructField) => field.nullable == false) shouldBe false
  }

  test("checkAnyFields plainly") {

    val schema = StructType((Seq(
      StructField("col1", StringType, false),
      StructField("col2", StringType, false),
      StructField("col3", ArrayType(StructType(Array(
        StructField("col31", StringType, false),
        StructField("col32", StructType(Seq(
          StructField("col321", StructType(Seq(
            StructField("col3211", StringType, false),
            StructField("col3212", StringType, false),
            StructField("col3213", StringType, false))), false),
          StructField("col322", ArrayType(StructType(Array(
            StructField("col3221", StringType, false),
            StructField("col3222", StringType, false))), false)))), false)))), false),
      StructField("col4", MapType(
        StringType,
        StructType(Seq(StructField("col4v", StringType, false))), false), false))))

    checkAnyFields(schema, (field: StructField) => field.nullable == true) shouldBe false
    schema.checkAnyFields((field: StructField) => field.nullable == true) shouldBe false
  }

  test("checkAnyFields inside arrays") {

    val schema = StructType((Seq(
      StructField("col1", StringType, false),
      StructField("col2", StringType, false),
      StructField("col3", ArrayType(StructType(Array(
        StructField("col31", StringType, false),
        StructField("col32", StructType(Seq(
          StructField("col321", StructType(Seq(
            StructField("col3211", StringType, false),
            StructField("col3212", StringType, true), // This one is true
            StructField("col3213", StringType, false))), false),
          StructField("col322", ArrayType(StructType(Array(
            StructField("col3221", StringType, false),
            StructField("col3222", StringType, false))), false)))), false)))), false),
      StructField("col4", MapType(
        StringType,
        StructType(Seq(StructField("col4v", StringType, false))), false), false))))

    checkAnyFields(schema, (field: StructField) => field.nullable == true) shouldBe true
    schema.checkAnyFields((field: StructField) => field.nullable == true) shouldBe true
  }

  test("checkAnyFields inside maps") {

    val schema = StructType((Seq(
      StructField("col1", StringType, false),
      StructField("col2", StringType, false),
      StructField("col3", ArrayType(StructType(Array(
        StructField("col31", StringType, false),
        StructField("col32", StructType(Seq(
          StructField("col321", StructType(Seq(
            StructField("col3211", StringType, false),
            StructField("col3212", StringType, false),
            StructField("col3213", StringType, false))), false),
          StructField("col322", ArrayType(StructType(Array(
            StructField("col3221", StringType, false),
            StructField("col3222", StringType, false))), false)))), false)))), false),
      StructField("col4", MapType(
        StringType,
        StructType(Seq(StructField("col4v", StringType, true))), false // This one is true
      ), false))))

    checkAnyFields(schema, (field: StructField) => field.nullable == true) shouldBe true
    schema.checkAnyFields((field: StructField) => field.nullable == true) shouldBe true
  }

}
