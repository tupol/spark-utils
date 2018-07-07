package org.tupol.spark.sql

import org.tupol.spark.SharedSparkSession
import org.tupol.spark.TestData.{ DummyData, DummyNestedData, DummyUnnestedData }
import org.scalatest.{ FunSuite, Matchers }

class DataFrameOpsSpec extends FunSuite with Matchers with SharedSparkSession {

  test("flattenFields on a flat DataFrame should produce no changes.") {

    val df = spark.createDataFrame(Seq[DummyData]())

    flattenFields(df).schema shouldBe df.schema

    df.flattenFields.schema shouldBe df.schema

  }

  test("flattenFields on a structured DataFrame should flatten it.") {

    val expectedSchema = schemaFor[DummyUnnestedData]

    val df = spark.createDataFrame(Seq[DummyNestedData]()).toDF

    flattenFields(df).schema shouldBe expectedSchema

    df.flattenFields.schema shouldBe expectedSchema

  }

}
