package org.tupol.spark.implicits

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.SharedSparkSession

class KeyValueDatasetOpsTest extends AnyWordSpec with SharedSparkSession with Matchers {

  import spark.implicits._

  "mapValues" should {
    "map only the values and not the keys" in {
      val input: Map[String, Int]         = Map(("a", 1), ("b", 2))
      val dataset: Dataset[(String, Int)] = spark.createDataset(input.toSeq)
      val result: Dataset[(String, Int)]  = dataset.mapValues(_ * 10)
      val expected                        = input.mapValues(_ * 10)
      result.collect() should contain theSameElementsAs (expected)
    }
    "return an empty dataset for an empty dataset" in {
      val result = spark.emptyDataset[(String, Int)].mapValues(_ + 1).collect()
      result.size shouldBe 0
    }
  }

  "flatMapValues" should {
    "flatMap only the values and not the keys" in {
      val input: Map[String, Int]         = Map(("a", 1), ("b", 2))
      val dataset: Dataset[(String, Int)] = spark.createDataset(input.toSeq)
      val result: Dataset[(String, Int)]  = dataset.flatMapValues(0 to _)
      val expected                        = Seq(("a", 0), ("a", 1), ("b", 0), ("b", 1), ("b", 2))
      result.collect() should contain theSameElementsAs (expected)
    }
    "return an empty dataset for an empty dataset" in {
      val result = spark.emptyDataset[(String, Int)].flatMapValues(Seq(0, _)).collect()
      result.size shouldBe 0
    }
  }

}
