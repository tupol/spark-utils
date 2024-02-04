package org.tupol.spark.sql

import org.tupol.spark.SharedSparkSession
import org.tupol.spark.TestData.{ DeepClass, TestClass }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.implicits._

class MapOpsRowOpsSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  test("Converting a map to a row and the conversion back from a row to a map") {
    val mapValue  = Map("a" -> 1, "b" -> 2)
    val deepValue = DeepClass(Seq((2, 2.2), (3, 3.3)))

    val data = Map("mapValue" -> mapValue, "deepValue" -> deepValue)

    val resultRow = data.toRow(schemaFor[TestClass])

    resultRow.getAs[Map[String, Int]]("mapValue") shouldBe mapValue
    resultRow.getAs[DeepClass]("deepValue") shouldBe deepValue

    resultRow.toMap shouldBe data
  }

}
