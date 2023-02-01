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


import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tupol.spark.Logging
import org.tupol.spark.io.sources.SourceConfiguration
import org.tupol.spark.io.{DataSource, DataSourceException, FormatType}
import org.tupol.utils.implicits._

import scala.util.Try

case class FileStreamDataSource(configuration: FileStreamDataSourceConfiguration) extends DataSource[FileStreamDataSourceConfiguration, DataStreamReader] with Logging {

  /** Create and configure a `DataStreamReader` based on the given `SourceConfiguration` */
  def reader(implicit spark: SparkSession): DataStreamReader = {

    val dataFormat = configuration.sourceConfiguration.format.toString
    val basicReader: DataStreamReader = spark.readStream
      .format(dataFormat)
      .options(configuration.sourceConfiguration.options)

    configuration.sourceConfiguration.schemaWithCorruptRecord match {
      case Some(schema) =>
        logDebug(s"Initializing the '$dataFormat' DataFrame loader using the specified schema.")
        basicReader.schema(schema)
      case None =>
        logDebug(s"Initializing the '$dataFormat' DataFrame loader inferring the schema.")
        basicReader
    }
  }

  /** Try to read the data according to the given configuration and return the read data or a failure */
  override def read(implicit spark: SparkSession): Try[DataFrame] = {
    logInfo(s"Reading data as '${configuration.sourceConfiguration.format}' from '${configuration.path}'.")
    Try(reader.load(configuration.path))
      .mapFailure(DataSourceException(s"Failed to read the data as '${configuration.sourceConfiguration.format}' from '${configuration.path}'", _))
      .logSuccess(d => logInfo(s"Successfully read the data as '${configuration.sourceConfiguration.format}' " +
        s"from '${configuration.path}'"))
      .logFailure(logError)
  }
}

case class FileStreamDataSourceConfiguration(path: String, sourceConfiguration: SourceConfiguration)
  extends FormatAwareStreamingSourceConfiguration {
  /** Get the format type of the input file. */
  def format: FormatType = sourceConfiguration.format
  override def toString: String = s"path: '$path', $sourceConfiguration"

  /** The options the can be set to the [[org.apache.spark.sql.DataFrameReader]] */
  override def options: Map[String, String] = sourceConfiguration.options

  /** The schema the can be set to the [[org.apache.spark.sql.DataFrameReader]] */
  override def schema: Option[StructType] = sourceConfiguration.schema
}
object FileStreamDataSourceConfiguration {
  import org.tupol.spark.io.FormatType._
  val AcceptableFileFormats = Seq(Csv, Json, Parquet, Orc, Text)
}