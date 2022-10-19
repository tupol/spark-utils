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
package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigRenderOptions
import org.apache.spark.sql.types.StructType
import org.tupol.spark.io.sources.{JdbcSourceConfiguration, _}
import org.tupol.spark.io.{BucketsConfiguration, DataSinkConfiguration, FileSinkConfiguration, FileSourceConfiguration, FormatAwareDataSinkConfiguration, FormatAwareDataSourceConfiguration, FormatType, GenericSinkConfiguration, JdbcSinkConfiguration, PartitionsConfiguration}
import org.tupol.spark.sql.loadSchemaFromString
import org.tupol.spark.utils.fuzzyLoadTextResourceFile
import pureconfig.{CamelCase, ConfigCursor, ConfigFieldMapping, ConfigObjectCursor, ConfigReader, ConfigSource}
import pureconfig.error.{CannotConvert, ConfigReaderFailures, FailureReason}
import org.tupol.utils.implicits._
import pureconfig.ConfigReader.Result
import pureconfig.generic.ProductHint
import pureconfig.generic.semiauto.deriveReader

object readers {

  implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

  final case class CanNotLoadResource(path: String, cause: Throwable) extends FailureReason {
    def description = s"Cannot load '$path': ${cause.getMessage}"
  }

  private def readerFailure(cursor: ConfigCursor, reason: FailureReason) =
    ConfigReaderFailures(cursor.failureFor(reason))

  /**
   * Configuration extractor for Schemas.
   *
   * It can be used as
   * `config.extract[Option[StructType]]("configuration_path_to_schema")` or as
   * `config.extract[StructType]("configuration_path_to_schema")`
   */
  implicit val StructTypeReader: ConfigReader[StructType] = ConfigReader.fromCursor[StructType] { cur =>
    val pathKey = "path"
    def fromPath(cur: ConfigCursor): ConfigReader.Result[StructType] =
      for {
        objCur  <- cur.asObjectCursor
        pathCur <- objCur.atKey(pathKey)
        path    <- pathCur.asString
        stringSchema <- fuzzyLoadTextResourceFile(path).toEither
          .mapLeft(t => readerFailure(cur, CanNotLoadResource(path, t)))
        schema <- loadSchemaFromString(stringSchema).toEither
          .mapLeft(t => readerFailure(cur, CannotConvert(stringSchema, "StructType", t.getMessage)))
      } yield schema

    def fromString(cur: ConfigCursor): ConfigReader.Result[StructType] = {
      for {
        objCur <- cur.asConfigValue
        stringSchema = objCur.render(ConfigRenderOptions.concise())
        schema <- loadSchemaFromString(stringSchema).toEither
          .mapLeft(t => readerFailure(cur, CannotConvert(stringSchema, "StructType", t.getMessage)))
      } yield schema
    }

    fromString(cur) match {
      case Left(fx) => fromPath(cur).mapLeft(_ ++ fx)
      case res      => res
    }
  }

  implicit val FormatTypeReader: ConfigReader[FormatType] = ConfigReader[String].emap { value =>
    FormatType.fromString(value).toEither
      .mapLeft(t => CannotConvert(value, "FormatType", t.getMessage))
  }

  private def genericSourceConfigurationFromCursor(objCur: ConfigObjectCursor): Result[GenericSourceConfiguration] =
    for {
      config <- objCur.asConfigValue.map(_.toConfig)
      genericConfig <- {
        import pureconfig.generic.auto._
        ConfigSource.fromConfig(config).load[GenericSourceConfiguration]
      }
    } yield genericConfig

  implicit val XmlSourceConfigurationReader: ConfigReader[XmlSourceConfiguration] =
    ConfigReader.fromCursor[XmlSourceConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        rowTag <- objCur.atKey("rowTag").flatMap(_.asString)
        config <- objCur.asConfigValue.map(_.toConfig)
        genericConfig <- {
          import pureconfig.generic.auto._
          ConfigSource.fromConfig(config).load[GenericSourceConfiguration]
        }
      } yield XmlSourceConfiguration(genericConfig, rowTag)
    }

  implicit val CsvSourceConfigurationReader: ConfigReader[CsvSourceConfiguration] =
    ConfigReader.fromCursor[CsvSourceConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        delimiter <- objCur.atKey("delimiter").flatMap(_.asString)
        header <- objCur.atKey("header").flatMap(_.asBoolean)
        config <- objCur.asConfigValue.map(_.toConfig)
        genericConfig <- {
          import pureconfig.generic.auto._
          ConfigSource.fromConfig(config).load[GenericSourceConfiguration]
        }
      } yield CsvSourceConfiguration(genericConfig, delimiter, header)
    }

  implicit val SourceConfigurationReader: ConfigReader[SourceConfiguration] =
    ConfigReader.fromCursor[SourceConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        formatCur <- objCur.atKey("format")
        format <- FormatTypeReader.from(formatCur)
        config <- objCur.asConfigValue.map(_.toConfig)
        sourceConfig <- {
          import pureconfig.generic.auto._
          def source = ConfigSource.fromConfig(config)
          format match {
            case FormatType.Xml => source.load[XmlSourceConfiguration]
            case FormatType.Csv => source.load[CsvSourceConfiguration]
            case FormatType.Json => source.load[JsonSourceConfiguration]
            case FormatType.Parquet => source.load[ParquetSourceConfiguration]
            case FormatType.Avro => source.load[AvroSourceConfiguration]
            case FormatType.Orc => source.load[OrcSourceConfiguration]
            case FormatType.Text => source.load[TextSourceConfiguration]
            case FormatType.Jdbc => source.load[JdbcSourceConfiguration]
            case _ => source.load[GenericSourceConfiguration]
          }
        }
      } yield sourceConfig
  }

  implicit val FileSourceConfigurationReader: ConfigReader[FileSourceConfiguration] =
    ConfigReader.fromCursor[FileSourceConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        formatCur <- objCur.atKey("format")
        format <- FormatTypeReader.from(formatCur)
        _ <- if(FormatType.AcceptableFileFormats.contains(format)) Right(())
              else formatCur.failed(new FailureReason {
                      override def description: String =
                      s"The provided format is unsupported for a file data source. " +
                        s"Supported formats are: ${FormatType.AcceptableFileFormats.mkString("'", "', '", "'")}"
                    })
        pathCur <- objCur.atKey("path")
        path <- pathCur.asString
        sourceConfig <- SourceConfigurationReader.from(objCur)
      } yield FileSourceConfiguration(path, sourceConfig)
    }


  private case class PrivateJdbcSourceConfiguration(url: String, table: String, user: Option[String], password: Option[String],
                                                    driver: Option[String], options: Option[Map[String, String]],
                                                    schema: Option[StructType])

  implicit val JdbcSourceConfigurationReader: ConfigReader[JdbcSourceConfiguration] = {
    import pureconfig.generic.auto._
    ConfigReader[PrivateJdbcSourceConfiguration]
      .emap(c => Right(JdbcSourceConfiguration(c.url, c.table, c.user, c.password, c.driver, c.options.getOrElse(Map()), c.schema)))
  }

  implicit val FormatAwareDataSourceConfigurationReader: ConfigReader[FormatAwareDataSourceConfiguration] = {
    import pureconfig.generic.auto._
    ConfigReader[FileSourceConfiguration]
      .orElse(ConfigReader[JdbcSourceConfiguration])
      .orElse(ConfigReader[GenericSourceConfiguration])
  }

  implicit val PartitionsConfigurationReader: ConfigReader[PartitionsConfiguration] =
    deriveReader[PartitionsConfiguration]
      .ensure(conf =>
        conf.number.map(_ > 0).getOrElse(true),
        conf => s"If specified, the partition.files should be a positive integer > 0, but it was ${conf.number}."
      )

  implicit val BucketsConfigurationReader: ConfigReader[BucketsConfiguration] = {
    deriveReader[BucketsConfiguration]
      .ensure(conf =>
        conf.number > 0,
        conf => s"The number of buckets must be a positive integer > 0, but it was ${conf.number}."
      )
      .ensure(conf =>
        conf.columns.size > 0,
        conf => s"At least one column needs to be specified for bucketing."
      )
  }

  implicit val FileSinkConfigurationReader: ConfigReader[FileSinkConfiguration] = {

    import org.tupol.spark.io.pureconf._
    deriveReader[FileSinkConfiguration]
      .ensure(conf => FormatType.AcceptableFileFormats.contains(conf.format),
        conf =>
          s"The provided format is unsupported for a file data source. " +
            s"Supported formats are: ${FormatType.AcceptableFileFormats.mkString("'", "', '", "'")}"

      )
  }

  implicit val FormatAwareDataSinkConfigurationReader: ConfigReader[FormatAwareDataSinkConfiguration] =
    ConfigReader.fromCursor[FormatAwareDataSinkConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        formatCur <- objCur.atKey("format")
        format <- FormatTypeReader.from(formatCur)
        config <- objCur.asConfigValue.map(_.toConfig)
        sinkConfig <- {
          import pureconfig.generic.auto._
          def source = ConfigSource.fromConfig(config)
          format match {
            case FormatType.Jdbc => source.load[JdbcSinkConfiguration]
            case _ if FormatType.AcceptableFileFormats.contains(format) => source.load[FileSinkConfiguration]
            case _ => source.load[GenericSinkConfiguration]
          }
        }
      } yield sinkConfig
    }


  implicit val DataSinkConfigurationReader: ConfigReader[DataSinkConfiguration] =
    FormatAwareDataSinkConfigurationReader.map(_.asInstanceOf[DataSinkConfiguration])

}
