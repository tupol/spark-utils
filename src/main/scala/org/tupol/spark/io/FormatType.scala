package org.tupol.spark.io

import scala.util.{ Failure, Success, Try }

sealed trait FormatType

object FormatType {

  private val XmlFormat = "com.databricks.spark.xml"
  private val CsvFormat = "csv"
  private val JsonFormat = "json"
  private val ParquetFormat = "parquet"
  private val AvroFormat = "com.databricks.spark.avro"
  private val OrcFormat = "orc"
  private val TextFormat = "text"
  private val JdbcFormat = "jdbc"

  def fromString(formatString: String): Try[FormatType] = formatString.trim match {
    case XmlFormat | "xml" => Success(Xml)
    case CsvFormat => Success(Csv)
    case JsonFormat => Success(Json)
    case ParquetFormat => Success(Parquet)
    case AvroFormat | "avro" => Success(Avro)
    case OrcFormat => Success(Orc)
    case TextFormat => Success(Text)
    case JdbcFormat => Success(Jdbc)
    case _ => Failure(new IllegalArgumentException(
      s"""Unknown format type '$formatString'. Available format types are:
         |${(FormatType.AvailableFormats.map(_.toString) :+ "avro" :+ "xml").mkString("'", "', '", "'")}. """.stripMargin))
  }

  val AvailableFormats = Seq(Xml, Csv, Json, Parquet, Avro, Orc, Text, Jdbc)
  val AcceptableFileFormats = Seq(Xml, Csv, Json, Parquet, Avro, Orc, Text)
  case object Xml extends FormatType { override def toString: String = XmlFormat }
  case object Csv extends FormatType { override def toString: String = CsvFormat }
  case object Json extends FormatType { override def toString: String = JsonFormat }
  case object Parquet extends FormatType { override def toString: String = ParquetFormat }
  case object Avro extends FormatType { override def toString: String = AvroFormat }
  case object Orc extends FormatType { override def toString: String = OrcFormat }
  case object Text extends FormatType { override def toString: String = TextFormat }
  case object Jdbc extends FormatType { override def toString: String = JdbcFormat }
}
