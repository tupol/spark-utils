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

import com.typesafe.config.{ Config, ConfigRenderOptions }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.tupol.spark.io.sources.JdbcSourceConfiguration
import org.tupol.spark.sql.loadSchemaFromString
import org.tupol.spark.utils.fuzzyLoadTextResourceFile
import org.tupol.utils.config.Extractor

/** Common IO utilities */
package object io {

  /** For things that should be aware of their format type */
  trait FormatAware {
    def format: FormatType
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

  /*
   * Configuration extractor for sources and sinks.
   *
   * It can be used as
   * `config.extract[Option[FileSourceConfiguration]]("configuration_path")` or as
   * `config.extract[FileSourceConfiguration]("configuration_path")`
   */
  implicit val FormatAwareDataSourceConfigExtractor = FormatAwareDataSourceConfiguration
  implicit val FormatAwareDataSinkConfigExtractor = FormatAwareDataSinkConfiguration
  implicit val DataSinkConfigExtractor = DataSinkConfiguration
  implicit val FileSourceConfigExtractor = FileSourceConfiguration
  implicit val FileSinkConfigExtractor = FileSinkConfiguration
  implicit val JdbcSourceConfigExtractor = JdbcSourceConfiguration
  implicit val JdbcSinkConfigExtractor = JdbcSinkConfiguration

  implicit val DataSourceFactory =
    new DataSourceFactory {
      override def apply[C <: DataSourceConfiguration](configuration: C): DataSource[C] =
        configuration match {
          //TODO There must be a better way to use the type system without the type cast
          case c: FileSourceConfiguration => FileDataSource(c).asInstanceOf[DataSource[C]]
          case c: JdbcSourceConfiguration => JdbcDataSource(c).asInstanceOf[DataSource[C]]
          case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
        }
    }

  implicit val DataAwareSinkFactory =
    new DataAwareSinkFactory {
      override def apply[C <: DataSinkConfiguration](configuration: C, data: DataFrame): DataAwareSink[C] =
        configuration match {
          //TODO There must be a better way to use the type system without the type cast
          case c: FileSinkConfiguration => FileDataAwareSink(c, data).asInstanceOf[DataAwareSink[C]]
          case c: JdbcSinkConfiguration => JdbcDataAwareSink(c, data).asInstanceOf[DataAwareSink[C]]
          case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
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
    def extract(config: Config, path: String): StructType = {
      val schemaFromPath = if (config.getConfig(path).hasPath("path")) {
        fuzzyLoadTextResourceFile(config.getConfig(path).getString("path")).toOption
      } else None

      val schema = schemaFromPath.getOrElse(config.getObject(path).render(ConfigRenderOptions.concise()))
      loadSchemaFromString(schema)
    }
  }

}
