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


import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType



package object sources {

  val ColumnNameOfCorruptRecord = "columnNameOfCorruptRecord"

  trait SourceConfiguration extends FormatAwareDataSourceConfiguration {
    /** The options the can be set to the [[org.apache.spark.sql.DataFrameReader]] */
    def options: Map[String, String]
    /** The schema the can be set to the [[org.apache.spark.sql.DataFrameReader]] */
    def schema: Option[StructType]
    /** If the parser supports storing the failed records, they will be stored in this column */
    def columnNameOfCorruptRecord: Option[String] = options.get(ColumnNameOfCorruptRecord)
    override def toString: String = {
      val optionsStr = if (options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
      val schemaStr = schema.map(_.prettyJson).getOrElse("not specified")
      s"format: '$format', options: {$optionsStr}, schema: $schemaStr"
    }
  }

  case class CsvSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Csv
    /** The csv parser does not support this feature */
    override val columnNameOfCorruptRecord = None
  }

  case class XmlSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
    extends SourceConfiguration {
    val format = FormatType.Xml
  }

  case class JsonSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Json
  }

  case class ParquetSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Parquet
  }


  case class OrcSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Orc
  }

  case class AvroSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Avro
  }

  case class TextSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Text
  }


  /**
   * Basic configuration for the `JdbcDataSource`
   * @param url
   * @param table
   * @param user
   * @param password
   * @param driver
   */
  case class JdbcSourceConfiguration(url: String, table: String, user: Option[String], password: Option[String],
    driver: Option[String], options: Map[String, String], schema: Option[StructType]) extends SourceConfiguration {
    val format = FormatType.Jdbc
    def readerOptions: Map[String, String] = {
      val userOption = user.map(v => Map("user" -> v)).getOrElse(Nil)
      val passwordOption = password.map(v => Map("password" -> v)).getOrElse(Nil)
      val driverOption = driver.map(v => Map("driver" -> v)).getOrElse(Nil)
      options + (JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table) ++
        userOption ++ passwordOption ++ driverOption
    }
    override def toString: String = {
      val optionsStr = if (readerOptions.isEmpty) ""
      else readerOptions.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
      s"format: '$format', url: '$url', table: '$table', connection properties: {$optionsStr}"
    }
  }

  case class GenericSourceConfiguration(format: FormatType, options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    override def toString: String = {
      val optionsStr = if (options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
      val schemaStr = schema.map(_.prettyJson).getOrElse("not specified")
      s"format: '$format', options: {$optionsStr}, schema: $schemaStr"
    }
  }

}
