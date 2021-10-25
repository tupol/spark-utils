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


import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.tupol.configz.Extractor
import org.tupol.spark.io.configz.sources._
import org.tupol.spark.io.configz.streaming.structured._
import org.tupol.spark.io.sources._
import org.tupol.spark.io.streaming.structured._
import org.tupol.spark.sql.loadSchemaFromString
import org.tupol.spark.utils.fuzzyLoadTextResourceFile

import scala.util.Try

/** Common IO utilities */
package object configz {

  /**
   * Configuration extractor for FormatType.
   *
   * It can be used as
   * `config.extract[Option[FormatType]]("configuration_path_to_format")` or as
   * `config.extract[FormatType]("configuration_path_to_format")`
   */
  implicit val FormatTypeExtractor = new Extractor[FormatType] {
    def extract(config: Config, path: String): Try[FormatType] = Try(FormatType.fromString(config.getString(path))).flatten
  }

//  /**
//   * Configuration extractor for Schemas.
//   *
//   * It can be used as
//   * `config.extract[Option[StructType]]("configuration_path_to_schema")` or as
//   * `config.extract[StructType]("configuration_path_to_schema")`
//   */
//  implicit val StructTypeExtractor = new Extractor[StructType] {
//    def extract(config: Config, path: String): Try[StructType] =
//      for {
//        schemaJson <- Try(config.getObject(path).render(ConfigRenderOptions.concise()))
//        schema <- loadSchemaFromString(schemaJson)
//      } yield schema
//  }

  /*
   * Configuration extractor for sources and sinks.
   *
   * It can be used as
   * `config.extract[Option[FileSourceConfiguration]]("configuration_path")` or as
   * `config.extract[FileSourceConfiguration]("configuration_path")`
   */
  implicit val FormatAwareDataSourceConfigExtractor = FormatAwareDataSourceConfigurator
  implicit val FormatAwareDataSinkConfigExtractor = FormatAwareDataSinkConfigurator
  implicit val DataSinkConfigExtractor = DataSinkConfigurator
  implicit val FileSourceConfigExtractor = FileSourceConfigurator
  implicit val FileSinkConfigExtractor = FileSinkConfigurator
  implicit val JdbcSourceConfigExtractor = JdbcSourceConfigurator
  implicit val JdbcSinkConfigExtractor = JdbcSinkConfigurator
  implicit val GenericSourceConfigExtractor = GenericSourceConfigurator
  implicit val GenericDataSinkConfigExtractor = GenericSinkConfigurator
  implicit val SourceConfigExtractor = SourceConfigurator

  implicit val FormatAwareStreamingSourceConfigExtractor = FormatAwareStreamingSourceConfigurator
  implicit val FormatAwareStreamingSinkConfigExtractor = FormatAwareStreamingSinkConfigurator
  implicit val GenericStreamDataSourceConfigurationExtractor = GenericStreamDataSourceConfigurator
  implicit val GenericStreamDataSinkConfigurationExtractor = GenericStreamDataSinkConfigurator
  implicit val FileStreamDataSourceConfigurationExtractor = FileStreamDataSourceConfigurator
  implicit val FileStreamDataSinkConfigurationExtractor = FileStreamDataSinkConfigurator
  implicit val KafkaStreamDataSourceConfigurationExtractor = KafkaStreamDataSourceConfigurator
  implicit val KafkaStreamDataSinkConfigurationExtractor = KafkaStreamDataSinkConfigurator

  implicit val DataSourceFactory =
    new DataSourceFactory {
      override def apply[C <: DataSourceConfiguration](configuration: C): DataSource[C] =
        configuration match {
          //TODO There must be a better way to use the type system without the type cast
          case c: FileSourceConfiguration => FileDataSource(c).asInstanceOf[DataSource[C]]
          case c: JdbcSourceConfiguration => JdbcDataSource(c).asInstanceOf[DataSource[C]]
          case c: GenericSourceConfiguration => GenericDataSource(c).asInstanceOf[DataSource[C]]
          case c: FileStreamDataSourceConfiguration => FileStreamDataSource(c).asInstanceOf[DataSource[C]]
          case c: KafkaStreamDataSourceConfiguration => KafkaStreamDataSource(c).asInstanceOf[DataSource[C]]
          case c: GenericStreamDataSourceConfiguration => GenericStreamDataSource(c).asInstanceOf[DataSource[C]]
          case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
        }
    }

  implicit val DataAwareSinkFactory =
    new DataAwareSinkFactory {
      override def apply[C <: DataSinkConfiguration, WO](configuration: C, data: DataFrame): DataAwareSink[C, WO] =
        data.isStreaming match {
          case false =>
            configuration match {
              //TODO There must be a better way to use the type system without the type cast
              case c: FileSinkConfiguration => FileDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WO]]
              case c: JdbcSinkConfiguration => JdbcDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WO]]
              case c: GenericSinkConfiguration => GenericDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WO]]
              case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
            }
          case true =>
            configuration match {
              //TODO There must be a better way to use the type system without the type cast
              case c: FileStreamDataSinkConfiguration => FileStreamDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WO]]
              case c: KafkaStreamDataSinkConfiguration => KafkaStreamDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WO]]
              case c: GenericStreamDataSinkConfiguration => GenericStreamDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WO]]
              case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
            }
        }
    }

  /**
   * Extended Configuration extractor for Schemas.
   *
   * This extractor will try first to get the schema from an external resources specified through a path.
   * If that fails it will try to load the schema straight from the given configuration.
   *
   * It can be used as
   * `config.extract[Option[StructType]]("configuration_path_to_schema")` or as
   * `config.extract[StructType]("configuration_path_to_schema")`
   */
  implicit val ExtendedStructTypeExtractor = new Extractor[StructType] {
    def extract(config: Config, path: String): Try[StructType] = {

      def schemaFromPath: Try[String] = {
        for {
          path <- Try(config.getConfig(path).getString("path"))
          stringSchema <- fuzzyLoadTextResourceFile(path)
        } yield stringSchema
      }
      def schemaFromConfig: Try[String] = Try(config.getObject(path).render(ConfigRenderOptions.concise()))

      for {
        stringSchema <- schemaFromPath orElse schemaFromConfig
        schema <- loadSchemaFromString(stringSchema)
      } yield schema

    }
  }

}
