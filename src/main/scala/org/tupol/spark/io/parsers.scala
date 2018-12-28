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
import org.apache.spark.sql.types.StructType
import org.tupol.utils.config.Configurator
import org.tupol.spark.implicits._
import scalaz.{ NonEmptyList, ValidationNel }

package object parsers {

  val ColumnNameOfCorruptRecord = "columnNameOfCorruptRecord"

  sealed trait ParserConfiguration {
    /** The options the can be set to the [[org.apache.spark.sql.DataFrameReader]] */
    def options: Map[String, String]
    /** The schema the can be set to the [[org.apache.spark.sql.DataFrameReader]] */
    def schema: Option[StructType]
    /** If the parser supports storing the failed records, they will be stored in this column */
    def columnNameOfCorruptRecord: Option[String] = options.get(ColumnNameOfCorruptRecord)
    /** The `FormatType` which corresponds to the format as required by the [[org.apache.spark.sql.DataFrameReader]] */
    final def format: FormatType = inputFormat(this)
  }
  object ParserConfiguration extends Configurator[ParserConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, ParserConfiguration] = {
      import org.tupol.utils.config._
      val format = config.extract[FormatType]("format")
      format match {
        case scalaz.Success(formatString) =>
          formatString match {
            case FormatType.Xml => XmlParserConfiguration.validationNel(config)
            case FormatType.Csv => CsvParserConfiguration.validationNel(config)
            case FormatType.Json => JsonParserConfiguration.validationNel(config)
            case FormatType.Parquet => ParquetConfiguration.validationNel(config)
            case FormatType.Avro => AvroConfiguration.validationNel(config)
            case FormatType.Orc => OrcConfiguration.validationNel(config)
            case FormatType.Text => TextConfiguration.validationNel(config)
          }
        case scalaz.Failure(e) =>
          scalaz.Failure[NonEmptyList[Throwable]](e)
      }
    }
  }

  case class CsvParserConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None) extends ParserConfiguration {
    /** The csv parser does not support this feature */
    override val columnNameOfCorruptRecord = None
  }
  object CsvParserConfiguration extends Configurator[CsvParserConfiguration] {
    def apply(options: Map[String, String], inputSchema: Option[StructType],
      delimiter: String, header: Boolean): CsvParserConfiguration =
      CsvParserConfiguration(
        options
          + ("delimiter" -> delimiter)
          + ("header" -> header.toString),
        inputSchema)
    override def validationNel(config: Config): ValidationNel[Throwable, CsvParserConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._

      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      val delimiter = config.extract[Option[String]]("delimiter").map(_.getOrElse(","))
      val header = config.extract[Option[Boolean]]("header").map(_.getOrElse(false))

      options |@| inputSchema |@| delimiter |@| header apply CsvParserConfiguration.apply
    }
  }

  case class XmlParserConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
    extends ParserConfiguration
  object XmlParserConfiguration extends Configurator[XmlParserConfiguration] {
    def apply(options: Map[String, String], inputSchema: Option[StructType],
      rowTag: String): XmlParserConfiguration =
      XmlParserConfiguration(
        options
          + ("rowTag" -> rowTag),
        inputSchema)
    override def validationNel(config: Config): ValidationNel[Throwable, XmlParserConfiguration] = {
      import org.tupol.utils.config._
      import scalaz.syntax.applicative._

      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      val rowTag = config.extract[String]("rowTag")

      options |@| inputSchema |@| rowTag apply XmlParserConfiguration.apply
    }
  }

  case class JsonParserConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None) extends ParserConfiguration
  object JsonParserConfiguration extends Configurator[JsonParserConfiguration] {
    def apply(basicConfig: BasicConfiguration) = new JsonParserConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, JsonParserConfiguration] =
      BasicConfiguration.validationNel(config) map JsonParserConfiguration.apply
  }

  case class ParquetConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None) extends ParserConfiguration
  object ParquetConfiguration extends Configurator[ParquetConfiguration] {
    def apply(basicConfig: BasicConfiguration) = new ParquetConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, ParquetConfiguration] =
      BasicConfiguration.validationNel(config) map ParquetConfiguration.apply
  }

  case class OrcConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None) extends ParserConfiguration
  object OrcConfiguration extends Configurator[OrcConfiguration] {
    def apply(basicConfig: BasicConfiguration) = new OrcConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, OrcConfiguration] =
      BasicConfiguration.validationNel(config) map OrcConfiguration.apply
  }

  case class AvroConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None) extends ParserConfiguration
  object AvroConfiguration extends Configurator[AvroConfiguration] {
    def apply(basicConfig: BasicConfiguration) = new AvroConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, AvroConfiguration] =
      BasicConfiguration.validationNel(config) map AvroConfiguration.apply
  }

  case class TextConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None) extends ParserConfiguration
  object TextConfiguration extends Configurator[TextConfiguration] {
    def apply(basicConfig: BasicConfiguration) = new TextConfiguration(basicConfig.options, basicConfig.schema)
    override def validationNel(config: Config): ValidationNel[Throwable, TextConfiguration] =
      BasicConfiguration.validationNel(config) map TextConfiguration.apply
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
