package io.tupol.spark.utils

import io.tupol.spark.SharedSparkSession
import org.scalatest.{ FunSuite, Matchers }

class MapOpsRowOpsSpec extends FunSuite with Matchers with SharedSparkSession {

  test("Converting a map to a row and the conversion back from a row to a map works") {
    val mapValue = Map("a" -> 1, "b" -> 2)
    val deepValue = DeepClass(Seq((2, 2.2), (3, 3.3)))

    val data = Map("mapValue" -> mapValue, "deepValue" -> deepValue)

    val resultRow = data.toRow(schemaFor[TestClass])

    resultRow.getAs[Map[String, Int]]("mapValue") shouldBe mapValue
    resultRow.getAs[DeepClass]("deepValue") shouldBe deepValue

    resultRow.toMap shouldBe data
  }

}

case class DeepClass(seqValue: Seq[(Int, Double)])

/**
 * We use this mainly to quickly generate the schema for our tests
 * @param mapValue
 * @param deepValue
 */
case class TestClass(mapValue: Map[String, Int], deepValue: DeepClass)
