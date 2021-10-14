package org.tupol.spark.io

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.ResultSet
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.implicits._
import org.tupol.spark.testing.H2Database

import scala.collection.mutable
import scala.util.Success

class JdbcDataSinkSpec extends AnyFunSuite with Matchers with SharedSparkSession with H2Database {

  val TestTable = "test_table"

  val TestData = Seq(
    JdbcTestRecord("v1", 1, 1.1, true),
    JdbcTestRecord("v2", 2, 2.2, false))

  test("Saving the input data results in the same data") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver)
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    val resultSet: ResultSet = connection.createStatement.executeQuery(s"SELECT * FROM $TestTable")
    val result = resultSetToJdbcTestRecords(resultSet)

    result should contain theSameElementsAs (TestData)
  }

  test("Saving the input data results in the same data with the overwrite save mode") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver, "overwrite", Map[String, String]())
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    val resultSet: ResultSet = connection.createStatement().executeQuery(s"SELECT * FROM $TestTable")
    val result = resultSetToJdbcTestRecords(resultSet)

    result should contain theSameElementsAs (TestData)
  }

  test("Saving the input data results in duplicated data with the append save mode") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver, "append", Map[String, String]())
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    val resultSet: ResultSet = connection.createStatement().executeQuery(s"SELECT * FROM $TestTable")
    val result = resultSetToJdbcTestRecords(resultSet)

    result should contain theSameElementsAs (TestData ++ TestData)
  }

  test("Saving the input data over the same table fails with default save mode") {

    val inputData = spark.createDataFrame(TestData)

    val sinkConfig = JdbcSinkConfiguration(h2url, TestTable, h2user, h2password, h2driver, "default", Map[String, String]())
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]
    inputData.sink(sinkConfig).write.failed.get shouldBe a[DataSinkException]
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
