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

import scala.util.{Success, Try}

sealed trait FormatType {
  val format: String
  override def toString: String = format
}

object FormatType {

  private val XmlFormat = "com.databricks.spark.xml"
  private val XmlFormatShort = "xml"
  private val CsvFormat = "csv"
  private val JsonFormat = "json"
  private val ParquetFormat = "parquet"
  private val AvroFormat = "com.databricks.spark.avro" // Starting Spark 4.x "avro" is part of org.apache.spark
  private val AvroFormatShort = "avro"
  private val OrcFormat = "orc"
  private val TextFormat = "text"
  private val DeltaFormat = "delta"
  private val JdbcFormat = "jdbc"
  private val SocketFormat = "socket"
  private val KafkaFormat = "kafka"

  def fromString(formatString: String): Try[FormatType] = formatString.toLowerCase.trim match {
    case XmlFormat | XmlFormatShort => Success(Xml)
    case CsvFormat => Success(Csv)
    case JsonFormat => Success(Json)
    case ParquetFormat => Success(Parquet)
    case AvroFormat | AvroFormatShort => Success(Avro)
    case OrcFormat => Success(Orc)
    case TextFormat => Success(Text)
    case DeltaFormat => Success(Delta)
    case JdbcFormat => Success(Jdbc)
    case SocketFormat => Success(Socket)
    case KafkaFormat => Success(Kafka)
    case _ => Success(Custom(formatString.trim))
  }

  val AcceptableFileFormats = Seq(Xml, Csv, Json, Parquet, Avro, Orc, Text, Delta)
  val AvailableFormats = AcceptableFileFormats :+ Jdbc
  val AcceptableStreamingFormats = AvailableFormats ++ Seq(Kafka, Socket)
  case object Xml extends FormatType { val format = XmlFormat }
  case object Csv extends FormatType { val format = CsvFormat }
  case object Json extends FormatType { val format = JsonFormat }
  case object Parquet extends FormatType { val format = ParquetFormat }
  case object Avro extends FormatType { val format = AvroFormat }
  case object Orc extends FormatType { val format = OrcFormat }
  case object Text extends FormatType { val format = TextFormat }
  case object Delta extends FormatType { val format = DeltaFormat }
  case object Jdbc extends FormatType { val format = JdbcFormat }
  case object Socket extends FormatType { val format = SocketFormat }
  case object Kafka extends FormatType { val format = KafkaFormat }
  case class Custom(format: String) extends FormatType
}
