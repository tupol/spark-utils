package org.tupol.spark.sql

import org.tupol.spark.SharedSparkSession
import org.tupol.spark.TestData.{ DeepClass, TestClass }
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.implicits._

class MapOpsRowOpsSpec extends FunSuite with Matchers with SharedSparkSession {

  test("Converting a map to a row and the conversion back from a row to a map") {
    val mapValue = Map("a" -> 1, "b" -> 2)
    val deepValue = DeepClass(Seq((2, 2.2), (3, 3.3)))

    val data = Map("mapValue" -> mapValue, "deepValue" -> deepValue)

    val resultRow = data.toRow(schemaFor[TestClass])

    resultRow.getAs[Map[String, Int]]("mapValue") shouldBe mapValue
    resultRow.getAs[DeepClass]("deepValue") shouldBe deepValue

    resultRow.toMap shouldBe data
  }

  test("Converting a Scala Map to a HashMap") {

    val scalaMap: Map[String, Int] = Map("a" -> 1, "b" -> 2)

    val hashMap: java.util.HashMap[String, Int] = scalaMap.toHashMap

    import scala.collection.JavaConverters._

    hashMap.entrySet.iterator.asScala.map(me => (me.getKey, me.getValue)).toMap should contain theSameElementsAs (scalaMap)

  }

}
