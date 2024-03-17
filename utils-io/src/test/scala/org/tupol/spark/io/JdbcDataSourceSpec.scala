package org.tupol.spark.io

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.JdbcSourceConfiguration
import org.tupol.spark.testing.H2Database

import java.sql.{ Connection, PreparedStatement }

class JdbcDataSourceSpec extends AnyFunSuite with Matchers with SharedSparkSession with H2Database {

  import spark.implicits._

  val TestTable = "test_table"

  val TestData = Seq(JdbcTestRecord("v1", 1, 1.1, true), JdbcTestRecord("v2", 2, 2.2, false))

  test("Reading the input data yields the correct result") {

    createTestTable(connection, TestData)

    val sourceConfig = JdbcSourceConfiguration(h2url, TestTable, h2user, h2password, h2driver)

    noException shouldBe thrownBy(spark.source(sourceConfig).read.get)

    spark.source(sourceConfig).read.get.as[JdbcTestRecord].collect() should contain theSameElementsAs (TestData)
  }

  test("Reading the input data fails if table can not be found") {

    val sourceConfig = JdbcSourceConfiguration(h2url, TestTable, h2user, h2password, h2driver)

    a[DataSourceException] should be thrownBy spark.source(sourceConfig).read.get
  }

  private def createTestTable(conection: Connection, testData: Seq[JdbcTestRecord]) = {
    connection.createStatement().executeUpdate(s"""CREATE TABLE $TestTable
                                                  |(
                                                  |  colString text,
                                                  |  colInt int,
                                                  |  colDouble double,
                                                  |  colBoolean boolean
                                                  |);""".stripMargin)
    val ps: PreparedStatement = connection.prepareStatement(
      s"INSERT INTO $TestTable (colString, colInt, colDouble, colBoolean) VALUES ( ?, ?, ?, ? );"
    )
    testData.foreach { r =>
      ps.setString(1, r.colString)
      ps.setInt(2, r.colInt)
      ps.setDouble(3, r.colDouble)
      ps.setBoolean(4, r.colBoolean)
      ps.executeUpdate()
    }
    connection.commit()

  }

}
