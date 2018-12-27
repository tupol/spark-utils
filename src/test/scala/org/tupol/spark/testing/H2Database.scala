package org.tupol.spark.testing

import java.sql.{ Connection, DriverManager }

import org.scalatest.{ BeforeAndAfterEach, Suite }

import scala.util.Try

trait H2Database extends BeforeAndAfterEach {
  this: Suite =>

  def h2driver = "org.h2.Driver"
  def h2database = "test"
  def h2url = s"jdbc:h2:~/$h2database"
  def h2user = ""
  def h2password = ""

  def openH2Connection: Connection = DriverManager.getConnection(h2url, h2user, h2password)
  def closeH2Connection(connection: Connection) = if (connection != null) connection.close()
  def dropDatabase() = if (connection != null) connection.createStatement.executeUpdate(s"DROP ALL OBJECTS DELETE FILES ;")

  var _connection: Connection = _

  def connection: Connection = _connection

  override def beforeEach(): Unit = {
    Class.forName(h2driver)
    Try(_connection = openH2Connection)
    //    Try(dropTestTable())
    Try(dropDatabase())
  }

  override def afterEach(): Unit = {
    super.afterEach()
    Try(dropDatabase())
    Try(closeH2Connection(connection))
  }
}
