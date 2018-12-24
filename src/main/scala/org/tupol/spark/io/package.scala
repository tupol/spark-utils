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
package org.tupol.spark

import com.typesafe.config.Config
import org.tupol.spark.io.parsers._
import org.tupol.utils.config.Extractor

import scala.util.{ Failure, Success, Try }

/** Common IO utilities */
package object io {

  sealed trait FormatType

  object FormatType {

    private val XmlFormat = "com.databricks.spark.xml"
    private val CsvFormat = "csv"
    private val JsonFormat = "json"
    private val ParquetFormat = "parquet"
    private val AvroFormat = "com.databricks.spark.avro"
    private val OrcFormat = "orc"
    private val TextFormat = "text"

    def fromString(formatString: String): Try[FormatType] = formatString.trim match {
      case XmlFormat | "xml" => Success(Xml)
      case CsvFormat => Success(Csv)
      case JsonFormat => Success(Json)
      case ParquetFormat => Success(Parquet)
      case AvroFormat | "avro" => Success(Avro)
      case OrcFormat => Success(Orc)
      case TextFormat => Success(Text)
      case _ => Failure(new IllegalArgumentException(
        s"""Unknown format type '$formatString'. Available format types are:
           |${FormatType.AvailableFormats.mkString("'", "', '", "'") :+ "avro" :+ "xml"}. """.stripMargin
      ))
    }

    val AvailableFormats = Seq(Xml, Csv, Json, Parquet, Avro, Orc, Text)
    case object Xml extends FormatType { override def toString: String = XmlFormat }
    case object Csv extends FormatType { override def toString: String = CsvFormat }
    case object Json extends FormatType { override def toString: String = JsonFormat }
    case object Parquet extends FormatType { override def toString: String = ParquetFormat }
    case object Avro extends FormatType { override def toString: String = AvroFormat }
    case object Orc extends FormatType { override def toString: String = OrcFormat }
    case object Text extends FormatType { override def toString: String = TextFormat }
  }

  def inputFormat[PC <: ParserConfiguration](config: PC): FormatType = config match {
    case _: XmlParserConfiguration => FormatType.Xml
    case _: CsvParserConfiguration => FormatType.Csv
    case _: JsonParserConfiguration => FormatType.Json
    case _: ParquetConfiguration => FormatType.Parquet
    case _: AvroConfiguration => FormatType.Avro
    case _: OrcConfiguration => FormatType.Orc
    case _: TextConfiguration => FormatType.Text
  }

  /**
   * Configuration extractor for FormatType.
   *
   * It can be used as
   * `config.extract[Option[FormatType]]("configuration_path_to_format")` or as
   * `config.extract[FormatType]("configuration_path_to_format")`
   */
  implicit val FormatTypeExtractor = new Extractor[FormatType] {
    def extract(config: Config, path: String): FormatType = {
      FormatType.fromString(config.getString(path)).get
    }
  }

}
