/*
MIT License

Copyright (c) 2018 Tupol (github.com/tupol)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package org.tupol.spark.io

import com.typesafe.config.Config
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{ DataFrame, DataFrameWriter, Row }
import org.tupol.spark.Logging
import org.tupol.utils.configz.Configurator
import org.tupol.utils.implicits._
import scalaz.ValidationNel

import scala.util.Try

/**  JdbcDataSink trait */
case class JdbcDataSink(configuration: JdbcSinkConfiguration) extends DataSink[JdbcSinkConfiguration, DataFrame] with Logging {

  /** Configure a `writer` for the given `DataFrame` based on the given `JdbcDataSinkConfig` */
  private def configureWriter(data: DataFrame, configuration: JdbcSinkConfiguration): DataFrameWriter[Row] = {
    data.write.format(configuration.format.toString).mode(configuration.saveMode).options(configuration.writerOptions)
  }

  /** Try to write the data according to the given configuration and return the same data or a failure */
  def write(data: DataFrame): DataFrame = {
    logInfo(s"Writing data as '${configuration.format}' " +
      s"to the '${configuration.table}' table of '${configuration.url}'.")
    val result = for {
      writer <- Try(configureWriter(data, configuration))
        .mapFailure(DataSinkException(s"Failed to create a '${configuration.format}' data writer " +
          s"(Full configuration: ${configuration}).", _))
        .logFailure(t => logError(t.getMessage, t))
      _ <- Try(writer.save())
        .mapFailure(DataSinkException(s"Failed to save the data as '${configuration.format}' " +
          s"to the '${configuration.table}' table of '${configuration.url}' " +
          s"(Full configuration: ${configuration}).", _))
        .logFailure(t => logError(t.getMessage, t))
    } yield data
    result.get
  }
}

/** JdbcDataSink trait that is data aware, so it can perform a write call with no arguments */
case class JdbcDataAwareSink(configuration: JdbcSinkConfiguration, data: DataFrame) extends DataAwareSink[JdbcSinkConfiguration, DataFrame] {
  override def sink: DataSink[JdbcSinkConfiguration, DataFrame] = JdbcDataSink(configuration)
}

/**
 * Basic configuration for the `JdbcDataSource`
 * @param url
 * @param table
 * @param user
 * @param password
 * @param driver
 */
case class JdbcSinkConfiguration(url: String, table: String, user: Option[String], password: Option[String],
  driver: Option[String], optionalSaveMode: Option[String], options: Map[String, String]) extends FormatAwareDataSinkConfiguration {
  val format = FormatType.Jdbc
  def saveMode = optionalSaveMode.getOrElse("default")
  def writerOptions: Map[String, String] = {
    val userOption = user.map(v => Map("user" -> v)).getOrElse(Nil)
    val passwordOption = password.map(v => Map("password" -> v)).getOrElse(Nil)
    val driverOption = driver.map(v => Map("driver" -> v)).getOrElse(Nil)
    options + (JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table) ++
      userOption ++ passwordOption ++ driverOption
  }

  override def toString: String = {
    val optionsStr = if (writerOptions.isEmpty) "" else writerOptions.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
    s"url: '$url', table: '$table', connection properties: {$optionsStr}"
  }
}
object JdbcSinkConfiguration extends Configurator[JdbcSinkConfiguration] {

  def apply(url: String, table: String, user: String, password: String,
    driver: String, saveMode: String, options: Map[String, String]): JdbcSinkConfiguration =
    new JdbcSinkConfiguration(url, table, Some(user), Some(password), Some(driver), Some(saveMode), options)
  def apply(url: String, table: String, user: String, password: String,
    driver: String, saveMode: Option[String] = None, options: Map[String, String] = Map()): JdbcSinkConfiguration =
    new JdbcSinkConfiguration(url, table, Some(user), Some(password), Some(driver), saveMode, options)

  override def validationNel(config: Config): ValidationNel[Throwable, JdbcSinkConfiguration] = {
    import org.tupol.utils.configz._
    import scalaz.syntax.applicative._
    config.extract[String]("url") |@|
      config.extract[String]("table") |@|
      config.extract[Option[String]]("user") |@|
      config.extract[Option[String]]("password") |@|
      config.extract[Option[String]]("driver") |@|
      config.extract[Option[String]]("mode") |@|
      config.extract[Map[String, String]]("options") apply
      JdbcSinkConfiguration.apply
  }
}
