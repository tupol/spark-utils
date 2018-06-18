package io.tupol.spark.utils

import io.tupol.spark.SharedSparkSession
import org.scalatest.{ FunSuite, Matchers }

class DataFrameOpsSpec extends FunSuite with Matchers with SharedSparkSession {

  test("flattenDataFrame on a flat DataFrame should produce no changes.") {

    val df = spark.createDataFrame(Seq[DummyData]())

    flattenDataFrame(df).schema shouldBe df.schema

    df.flatten.schema shouldBe df.schema

  }

  test("flattenDataFrame on a structured DataFrame should flatten it.") {

    val expectedSchema = schemaFor[DummyunnestedData]

    val df = spark.createDataFrame(Seq[DummyNestedData]()).toDF

    flattenDataFrame(df).schema shouldBe expectedSchema

    df.flatten.schema shouldBe expectedSchema

  }

}

case class DummyData(value: String)

case class DummyNestedData(value: String, dummyData: DummyData)

case class DummyunnestedData(value: String, dummyData_value: String)

object DummyFunction extends Function1[String, Int] with Serializable {
  override def apply(str: String): Int = str.length
}

