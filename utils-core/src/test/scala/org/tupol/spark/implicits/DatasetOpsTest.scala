package org.tupol.spark.implicits

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{AnalysisException, Dataset}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.SharedSparkSession

class DatasetOpsTest extends AnyWordSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  "withTupledColumn" should {
    "return a tuple of input and column with a simple dataset of 1 value" in {
      val input                               = Seq(Test1Val(1), Test1Val(2))
      val dataset: Dataset[Test1Val]          = spark.createDataset(input)
      val result: Dataset[(Test1Val, String)] = dataset.withColumnDataset[String](lit("test"))
      val expected                            = input.map { case t => (t, "test") }
      result.collect() should contain theSameElementsAs (expected)
    }
    "return a tuple of input and column with a simple dataset of 2 values" in {
      val input                               = Seq(Test2Val("a", 1), Test2Val("b", 2))
      val dataset: Dataset[Test2Val]          = spark.createDataset(input)
      val result: Dataset[(Test2Val, String)] = dataset.withColumnDataset[String](lit("test"))
      val expected                            = input.map { case t => (t, "test") }
      result.collect() should contain theSameElementsAs (expected)
    }
    "return a tuple of input and column with a simple dataset of nested values" in {
      val input                               = Seq(Test2Val("a", 1), Test2Val("b", 2)).map(tv2 => TestNest(Test1Val(tv2.value * 10), tv2))
      val dataset: Dataset[TestNest]          = spark.createDataset(input)
      val result: Dataset[(TestNest, String)] = dataset.withColumnDataset[String](lit("test"))
      val expected                            = input.map { case t => (t, "test") }
      result.collect() should contain theSameElementsAs (expected)
    }
    "return an empty dataset for an empty dataset" in {
      val result: Dataset[(Test2Val, String)] = spark.emptyDataset[Test2Val].withColumnDataset[String](lit("test"))
      result.count shouldBe 0
    }
    "fail if the specified column type does not match the actual column type" in {
      an[AnalysisException] shouldBe thrownBy(spark.emptyDataset[Test2Val].withColumnDataset[Int](lit("test")))
    }
  }

}

case class Test1Val(value: Int)
case class Test2Val(key: String, value: Int)
case class TestNest(v1: Test1Val, v2: Test2Val)
