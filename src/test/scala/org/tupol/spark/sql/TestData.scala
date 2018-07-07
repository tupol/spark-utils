package org.tupol.spark

object TestData {

  case class DummyData(value: String)

  case class DummyNestedData(value: String, dummyData: DummyData)

  case class DummyUnnestedData(value: String, dummyData_value: String)

  case class DeepClass(seqValue: Seq[(Int, Double)])

  /**
   * We use this mainly to quickly generate the schema for our tests
   * @param mapValue
   * @param deepValue
   */
  case class TestClass(mapValue: Map[String, Int], deepValue: DeepClass)

}
