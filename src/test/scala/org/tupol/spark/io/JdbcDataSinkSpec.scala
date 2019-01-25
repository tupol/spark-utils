package org.tupol.spark.io

import java.sql.ResultSet

import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.testing.H2Database

import scala.collection.mutable

class JdbcDataSinkSpec extends FunSuite with Matchers with SharedSparkSession with H2Database {

  val TestTable = "test_table"

  val TestData = Seq(
    JdbcTestRecord("v1", 1, 1.1, true),
    JdbcTestRecord("v2", 2, 2.2, false))

  test("Saving the input data results in the same data") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver)
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val resultSet: ResultSet = connection.createStatement.executeQuery(s"SELECT * FROM $TestTable")
    val result = resultSetToJdbcTestRecords(resultSet)

    result should contain theSameElementsAs (TestData)
  }

  test("Saving the input data results in the same data with the overwrite save mode") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver, "overwrite", Map[String, String]())
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val resultSet: ResultSet = connection.createStatement().executeQuery(s"SELECT * FROM $TestTable")
    val result = resultSetToJdbcTestRecords(resultSet)

    result should contain theSameElementsAs (TestData)
  }

  test("Saving the input data results in duplicated data with the append save mode") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver, "append", Map[String, String]())
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val resultSet: ResultSet = connection.createStatement().executeQuery(s"SELECT * FROM $TestTable")
    val result = resultSetToJdbcTestRecords(resultSet)

    result should contain theSameElementsAs (TestData ++ TestData)
  }

  test("Saving the input data over the same table fails with default save mode") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver, "default", Map[String, String]())
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)
    a[DataSinkException] should be thrownBy (inputData.sink(sinkConfig).write)
  }

  private def resultSetToJdbcTestRecords(resultSet: ResultSet) = {
    val resultsBuffer: mutable.ArrayBuffer[JdbcTestRecord] = mutable.ArrayBuffer[JdbcTestRecord]()
    while (resultSet.next) {
      resultsBuffer += JdbcTestRecord(
        resultSet.getString("colString"),
        resultSet.getInt("colInt"),
        resultSet.getDouble("colDouble"),
        resultSet.getBoolean("colBoolean"))
    }
    resultsBuffer
  }

}

case class JdbcTestRecord(colString: String, colInt: Int, colDouble: Double, colBoolean: Boolean)
