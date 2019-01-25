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
import org.apache.spark.sql.types.StructType
import org.tupol.utils.config.Configurator
import scalaz.{ NonEmptyList, ValidationNel }

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
  object SourceConfiguration extends Configurator[SourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, SourceConfiguration] = {
      import org.tupol.utils.config._
      val format = config.extract[FormatType]("format")
      format match {
        case scalaz.Success(formatString) =>
          formatString match {
            case FormatType.Xml => XmlSourceConfiguration.validationNel(config)
            case FormatType.Csv => CsvSourceConfiguration.validationNel(config)
            case FormatType.Json => JsonSourceConfiguration.validationNel(config)
            case FormatType.Parquet => ParquetSourceConfiguration.validationNel(config)
            case FormatType.Avro => AvroSourceConfiguration.validationNel(config)
            case FormatType.Orc => OrcSourceConfiguration.validationNel(config)
            case FormatType.Text => TextSourceConfiguration.validationNel(config)
            case FormatType.Jdbc => JdbcSourceConfiguration.validationNel(config)
            case unsupportedFormat => scalaz.Failure[NonEmptyList[Throwable]](new IllegalArgumentException(s"Unsupported format '$unsupportedFormat'").toNel)
          }
        case scalaz.Failure(e) =>
          scalaz.Failure[NonEmptyList[Throwable]](e)
      }
    }
  }

  case class CsvSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Csv
    /** The csv parser does not support this feature */
    override val columnNameOfCorruptRecord = None
  }
  object CsvSourceConfiguration extends Configurator[CsvSourceConfiguration] {
    def apply(options: Map[String, String], inputSchema: Option[StructType], delimiter: String, header: Boolean): CsvSourceConfiguration =
      CsvSourceConfiguration(
        options + ("delimiter" -> delimiter) + ("header" -> header.toString), inputSchema)
    override def validationNel(config: Config): ValidationNel[Throwable, CsvSourceConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._

      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      val delimiter = config.extract[Option[String]]("delimiter").map(_.getOrElse(","))
      val header = config.extract[Option[Boolean]]("header").map(_.getOrElse(false))

      options |@| inputSchema |@| delimiter |@| header apply CsvSourceConfiguration.apply
    }
  }

  case class XmlSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
    extends SourceConfiguration {
    val format = FormatType.Xml
  }
  object XmlSourceConfiguration extends Configurator[XmlSourceConfiguration] {
    def apply(options: Map[String, String], inputSchema: Option[StructType], rowTag: String): XmlSourceConfiguration =
      XmlSourceConfiguration(options + ("rowTag" -> rowTag), inputSchema)
    override def validationNel(config: Config): ValidationNel[Throwable, XmlSourceConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._

      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      val rowTag = config.extract[String]("rowTag")

      options |@| inputSchema |@| rowTag apply XmlSourceConfiguration.apply
    }
  }

  case class JsonSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Json
  }
  object JsonSourceConfiguration extends Configurator[JsonSourceConfiguration] {
    def apply(basicConfig: BasicConfiguration) =
      new JsonSourceConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, JsonSourceConfiguration] =
      BasicConfiguration.validationNel(config) map JsonSourceConfiguration.apply
  }

  case class ParquetSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Parquet
  }
  object ParquetSourceConfiguration extends Configurator[ParquetSourceConfiguration] {
    def apply(basicConfig: BasicConfiguration) =
      new ParquetSourceConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, ParquetSourceConfiguration] =
      BasicConfiguration.validationNel(config) map ParquetSourceConfiguration.apply
  }

  case class OrcSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Orc
  }
  object OrcSourceConfiguration extends Configurator[OrcSourceConfiguration] {
    def apply(basicConfig: BasicConfiguration) =
      new OrcSourceConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, OrcSourceConfiguration] =
      BasicConfiguration.validationNel(config) map OrcSourceConfiguration.apply
  }

  case class AvroSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Avro
  }
  object AvroSourceConfiguration extends Configurator[AvroSourceConfiguration] {
    def apply(basicConfig: BasicConfiguration) =
      new AvroSourceConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, AvroSourceConfiguration] =
      BasicConfiguration.validationNel(config) map AvroSourceConfiguration.apply
  }

  case class TextSourceConfiguration(
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Text
  }
  object TextSourceConfiguration extends Configurator[TextSourceConfiguration] {
    def apply(basicConfig: BasicConfiguration) =
      new TextSourceConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, TextSourceConfiguration] =
      BasicConfiguration.validationNel(config) map TextSourceConfiguration.apply
  }

  /**
   * Basic configuration for the `JdbcDataSource`
   * @param url
   * @param table
   * @param user
   * @param password
   * @param driver
   * @param properties
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
  object JdbcSourceConfiguration extends Configurator[JdbcSourceConfiguration] {
    def apply(url: String, table: String, user: String, password: String,
      driver: String, options: Map[String, String] = Map(),
      schema: Option[StructType] = None): JdbcSourceConfiguration =
      new JdbcSourceConfiguration(url, table, Some(user), Some(password), Some(driver), options, schema)

    override def validationNel(config: Config): ValidationNel[Throwable, JdbcSourceConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._
      config.extract[String]("url") |@|
        config.extract[String]("table") |@|
        config.extract[Option[String]]("user") |@|
        config.extract[Option[String]]("password") |@|
        config.extract[Option[String]]("driver") |@|
        config.extract[Option[Map[String, String]]]("options")
        .map(_.getOrElse(Map[String, String]())) |@|
        config.extract[Option[StructType]]("schema") apply
        JdbcSourceConfiguration.apply
    }
  }

  private case class BasicConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
  private object BasicConfiguration extends Configurator[BasicConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, BasicConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._
      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      options |@| inputSchema apply BasicConfiguration.apply
    }
  }
}
