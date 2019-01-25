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
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SparkSession }
import org.tupol.spark.Logging
import org.tupol.spark.io.sources._
import org.tupol.utils._
import org.tupol.utils.config.Configurator
import scalaz.{ NonEmptyList, ValidationNel }

import scala.util.{ Failure, Success, Try }

case class FileDataSource(configuration: FileSourceConfiguration) extends DataSource[FileSourceConfiguration] with Logging {

  /** Create and configure a `DataFrameReader` based on the given `SourceConfiguration` */
  private def createReader(sourceConfiguration: SourceConfiguration)(implicit spark: SparkSession): DataFrameReader = {

    val dataFormat = sourceConfiguration.format.toString
    val basicReader = spark.read
      .format(dataFormat)
      .options(sourceConfiguration.options)

    sourceConfiguration.schema match {
      case Some(inputSchema) =>
        logDebug(s"Initializing the '$dataFormat' DataFrame loader using the specified schema.")
        val schema = sourceConfiguration.columnNameOfCorruptRecord
          .map { columnNameOfCorruptRecord =>
            logDebug(s"The '$ColumnNameOfCorruptRecord' was specified; adding column '$columnNameOfCorruptRecord' to the input schema.")
            inputSchema.add(columnNameOfCorruptRecord, StringType)
          }
          .getOrElse(inputSchema)
        basicReader.schema(schema)
      case None =>
        logDebug(s"Initializing the '$dataFormat' DataFrame loader inferring the schema.")
        basicReader
    }
  }

  /** Try to read the data according to the given configuration and return the read data or a failure */
  def read(implicit spark: SparkSession): DataFrame = {
    logInfo(s"Reading data as '${configuration.sourceConfiguration.format}' from '${configuration.path}'.")
    Try(createReader(configuration.sourceConfiguration).load(configuration.path))
      .logSuccess(d => logInfo(s"Successfully read the data as '${configuration.sourceConfiguration.format}' from '${configuration.path}'")) match {
        case Failure(t) =>
          val message = s"Failed to read the data as '${configuration.sourceConfiguration.format}' from '${configuration.path}'"
          logError(message, t)
          throw new DataSourceException(message, t)
        case Success(s) => s
      }
  }

}

/**
 * Basic configuration for the `FileDataSource``
 * @param path
 * @param sourceConfiguration
 */
case class FileSourceConfiguration(path: String, sourceConfiguration: SourceConfiguration) extends FormatAwareDataSourceConfiguration {
  /** Get the format type of the input file. */
  def format: FormatType = sourceConfiguration.format
  override def toString: String = s"path: '$path', source configuration: { $sourceConfiguration }"
}
object FileSourceConfiguration extends Configurator[FileSourceConfiguration] {
  override def validationNel(config: Config): ValidationNel[Throwable, FileSourceConfiguration] = {
    import org.tupol.utils.config._
    import scalaz.syntax.applicative._

    val format = config.extract[FormatType]("format").ensure(
      new IllegalArgumentException(s"The provided format is unsupported for a file data source. " +
        s"Supported formats are: ${FormatType.AcceptableFileFormats.mkString("'", "', '", "'")}").toNel)(f => FormatType.AcceptableFileFormats.contains(f))

    format match {
      case scalaz.Success(_) =>
        config.extract[String]("path") |@|
          SourceConfiguration.validationNel(config) apply
          FileSourceConfiguration.apply
      case scalaz.Failure(e) =>
        scalaz.Failure[NonEmptyList[Throwable]](e)
    }

  }
}
