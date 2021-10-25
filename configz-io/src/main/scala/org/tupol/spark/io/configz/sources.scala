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
package org.tupol.spark.io.configz

import org.tupol.spark.io._
import org.tupol.spark.io.sources._
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import org.tupol.configz._
import scalaz.{ NonEmptyList, ValidationNel }


package object sources {

  object SourceConfigurator extends Configurator[SourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, SourceConfiguration] = {
      import org.tupol.configz._
      val format = config.extract[FormatType]("format")
      format match {
        case scalaz.Success(formatString) =>
          formatString match {
            case FormatType.Xml => XmlSourceConfigurator.validationNel(config)
            case FormatType.Csv => CsvSourceConfigurator.validationNel(config)
            case FormatType.Json => JsonSourceConfigurator.validationNel(config)
            case FormatType.Parquet => ParquetSourceConfigurator.validationNel(config)
            case FormatType.Avro => AvroSourceConfigurator.validationNel(config)
            case FormatType.Orc => OrcSourceConfigurator.validationNel(config)
            case FormatType.Text => TextSourceConfigurator.validationNel(config)
            case FormatType.Jdbc => JdbcSourceConfigurator.validationNel(config)
            case _ => GenericSourceConfigurator.validationNel(config)
          }
        case scalaz.Failure(e) =>
          scalaz.Failure[NonEmptyList[Throwable]](e)
      }
    }
  }

  object CsvSourceConfigurator extends Configurator[CsvSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, CsvSourceConfiguration] = {
      import org.tupol.configz._
      import scalaz.syntax.applicative._

      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      val delimiter = config.extract[Option[String]]("delimiter").map(_.getOrElse(","))
      val header = config.extract[Option[Boolean]]("header").map(_.getOrElse(false))

      options |@| inputSchema |@| delimiter |@| header apply CsvSourceConfiguration.apply
    }
  }

  object XmlSourceConfigurator extends Configurator[XmlSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, XmlSourceConfiguration] = {
      import org.tupol.configz._
      import scalaz.syntax.applicative._

      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      val rowTag = config.extract[String]("rowTag")

      options |@| inputSchema |@| rowTag apply XmlSourceConfiguration.apply
    }
  }

  object JsonSourceConfigurator extends Configurator[JsonSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, JsonSourceConfiguration] =
      GenericSourceConfigurator.validationNel(config) map JsonSourceConfiguration.apply
  }


  object ParquetSourceConfigurator extends Configurator[ParquetSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, ParquetSourceConfiguration] =
      GenericSourceConfigurator.validationNel(config) map ParquetSourceConfiguration.apply
  }

  object OrcSourceConfigurator extends Configurator[OrcSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, OrcSourceConfiguration] =
      GenericSourceConfigurator.validationNel(config) map OrcSourceConfiguration.apply
  }

  object AvroSourceConfigurator extends Configurator[AvroSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, AvroSourceConfiguration] =
      GenericSourceConfigurator.validationNel(config) map AvroSourceConfiguration.apply
  }

  object TextSourceConfigurator extends Configurator[TextSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, TextSourceConfiguration] =
      GenericSourceConfigurator.validationNel(config) map TextSourceConfiguration.apply
  }

  object JdbcSourceConfigurator extends Configurator[JdbcSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, JdbcSourceConfiguration] = {
      import org.tupol.configz._
      import scalaz.syntax.applicative._
      config.extract[String]("url") |@|
        config.extract[String]("table") |@|
        config.extract[Option[String]]("user") |@|
        config.extract[Option[String]]("password") |@|
        config.extract[Option[String]]("driver") |@|
        config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]())) |@|
        config.extract[Option[StructType]]("schema") apply
        JdbcSourceConfiguration.apply
    }
  }


  object GenericSourceConfigurator extends Configurator[GenericSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, GenericSourceConfiguration] = {
      import org.tupol.configz._
      import scalaz.syntax.applicative._
      val options = config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]()))
      val inputSchema = config.extract[Option[StructType]]("schema")
      val format = config.extract[FormatType]("format")

      format |@| options |@| inputSchema apply GenericSourceConfiguration.apply
    }
  }
}
