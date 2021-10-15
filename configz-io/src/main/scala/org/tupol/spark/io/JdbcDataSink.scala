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
import org.tupol.configz.Configurator
import org.tupol.utils.implicits._
import scalaz.ValidationNel

import scala.util.Try

object JdbcSinkConfiguration extends Configurator[JdbcSinkConfiguration] {

  def apply(url: String, table: String, user: String, password: String,
    driver: String, saveMode: String, options: Map[String, String]): JdbcSinkConfiguration =
    new JdbcSinkConfiguration(url, table, Some(user), Some(password), Some(driver), Some(saveMode), options)
  def apply(url: String, table: String, user: String, password: String,
    driver: String, saveMode: Option[String] = None, options: Map[String, String] = Map()): JdbcSinkConfiguration =
    new JdbcSinkConfiguration(url, table, Some(user), Some(password), Some(driver), saveMode, options)

  override def validationNel(config: Config): ValidationNel[Throwable, JdbcSinkConfiguration] = {
    import org.tupol.configz._
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
