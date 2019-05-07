package org.tupol.spark.implicits

import org.scalatest.{ FunSuite, Matchers }

class ProductOpsSpec extends FunSuite with Matchers {

  test("Simple case class toMap") {
    val cc1 = CC1(1, "1", Seq("1", "2"))
    val expected = Map("intProp" -> 1, "stringProp" -> "1", "seqProp" -> Seq("1", "2"))
    cc1.toMap shouldBe expected
  }

  test("Inner case class toMap") {
    val cc1 = CC1(1, "1", Seq("1", "2"))
    val cc2 = CC2(1.1, true, cc1)
    val expected = Map(
      "doubleProp" -> 1.1,
      "boolProp" -> true,
      "cc1Prop" -> Map("intProp" -> 1, "stringProp" -> "1", "seqProp" -> Seq("1", "2")))
    cc2.toMap shouldBe expected
  }

  case class CC1(intProp: Int, stringProp: String, seqProp: Seq[String])
  case class CC2(doubleProp: Double, boolProp: Boolean, cc1Prop: CC1)
}
