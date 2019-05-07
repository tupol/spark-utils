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
package org.tupol.spark.io.streaming.structured

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.tupol.spark.Logging
import org.tupol.spark.io.FormatType._
import org.tupol.spark.io.sources.{ ColumnNameOfCorruptRecord, SourceConfiguration }
import org.tupol.spark.io.{ DataSource, DataSourceException, FormatType }
import org.tupol.utils._
import org.tupol.utils.config.Configurator
import scalaz.{ NonEmptyList, ValidationNel }

import scala.util.{ Failure, Success, Try }

case class FileStreamDataSource(configuration: FileStreamDataSourceConfiguration) extends DataSource[FileStreamDataSourceConfiguration] with Logging {

  /** Create and configure a `DataStreamReader` based on the given `SourceConfiguration` */
  private def createReader(sourceConfiguration: SourceConfiguration)(implicit spark: SparkSession): DataStreamReader = {

    val dataFormat = sourceConfiguration.format.toString
    val basicReader: DataStreamReader = spark.readStream
      .format(dataFormat)
      .options(sourceConfiguration.options)

    sourceConfiguration.schema match {
      case Some(inputSchema) =>
        logDebug(s"Initializing the '$dataFormat' DataStreamReader using the specified schema.")
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
      .logSuccess(d => logInfo(s"Successfully read the data as '${configuration.sourceConfiguration.format}' " +
        s"from '${configuration.path}'")) match {
        case Failure(t) =>
          val message = s"Failed to read the data as '${configuration.sourceConfiguration.format}' from '${configuration.path}'"
          logError(message, t)
          throw new DataSourceException(message, t)
        case Success(x) => x
      }
  }
}

/**
 * Basic configuration for the `FileDataSource`
 * @param path
 * @param sourceConfiguration
 */
case class FileStreamDataSourceConfiguration(path: String, sourceConfiguration: SourceConfiguration)
  extends FormatAwareStreamingSourceConfiguration {
  /** Get the format type of the input file. */
  def format: FormatType = sourceConfiguration.format
  override def toString: String = s"path: '$path', source configuration: { $sourceConfiguration }"
}
object FileStreamDataSourceConfiguration extends Configurator[FileStreamDataSourceConfiguration] {
  val AcceptableFileFormats = Seq(Csv, Json, Parquet, Orc, Text)
  override def validationNel(config: Config): ValidationNel[Throwable, FileStreamDataSourceConfiguration] = {
    import org.tupol.utils.config._
    import scalaz.syntax.applicative._

    val format = config.extract[FormatType]("format").ensure(
      new IllegalArgumentException(s"The provided format is unsupported for a file data source. " +
        s"Supported formats are: ${FormatType.AcceptableFileFormats.mkString("'", "', '", "'")}").toNel)(f => FormatType.AcceptableFileFormats.contains(f))

    format match {
      case scalaz.Success(_) =>
        config.extract[String]("path") |@|
          SourceConfiguration.validationNel(config) apply
          FileStreamDataSourceConfiguration.apply
      case scalaz.Failure(e) =>
        scalaz.Failure[NonEmptyList[Throwable]](e)
    }
  }
}
