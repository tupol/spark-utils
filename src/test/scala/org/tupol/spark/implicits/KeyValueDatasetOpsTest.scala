package org.tupol.spark.implicits

import org.apache.spark.sql.Dataset
import org.scalatest.{ Matchers, WordSpec }
import org.tupol.spark.SharedSparkSession

class KeyValueDatasetOpsTest extends WordSpec with Matchers with SharedSparkSession {

  import spark.implicits._
  "mapValues" should {
    "map only the values and not the keys" in {
      val input = Map(("a", 1), ("b", 2))
      val dataset: Dataset[(String, Int)] = spark.createDataset(input.toSeq)
      val result = dataset.mapValues(_ * 10).collect()
      val expected = input.mapValues(_ * 10)
      result should contain theSameElementsAs (expected)
    }
    "return an empty dataset for an empty dataset" in {
      val result = spark.emptyDataset[(String, Int)].mapValues(_ + 1).collect()
      result.size shouldBe 0
    }
  }

  "flatMapValues" should {
    "flatMap only the values and not the keys" in {
      val input = Map(("a", 1), ("b", 2))
      val dataset: Dataset[(String, Int)] = spark.createDataset(input.toSeq)
      val result = dataset.flatMapValues(Seq(0, _)).collect()
      val expected = Seq(("a", 0), ("a", 1), ("b", 0), ("b", 2))
      result should contain theSameElementsAs (expected)
    }
    "return an empty dataset for an empty dataset" in {
      val result = spark.emptyDataset[(String, Int)].flatMapValues(Seq(0, _)).collect()
      result.size shouldBe 0
    }
  }

}
