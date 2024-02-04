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
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.tupol.spark.Logging
import org.tupol.spark.io._
import org.tupol.utils.implicits._

import scala.util.Try

case class GenericStreamDataSource(configuration: GenericStreamDataSourceConfiguration)
    extends DataSource[GenericStreamDataSourceConfiguration, DataStreamReader]
    with Logging {

  /** Create and configure a `DataStreamReader` based on the given `GenericStreamDataSourceConfiguration` */
  def reader(implicit spark: SparkSession): DataStreamReader = {

    val dataFormat = configuration.format.toString
    val basicReader = spark.readStream
      .format(dataFormat)
    val readerWithOptions = basicReader.options(configuration.options)
    readerWithOptions
  }

  /** Try to read the data according to the given configuration and return the read data or a failure */
  override def read(implicit spark: SparkSession): Try[DataFrame] = {
    logInfo(s"Reading data as '${configuration.format}' from '${configuration}'.")
    Try(reader.load())
      .mapFailure(
        DataSourceException(s"Failed to read the data as '${configuration.format}' from '${configuration}'", _)
      )
      .logSuccess(_ => logInfo(s"Successfully read the data as '${configuration.format}' from '${configuration}'"))
      .logFailure(logError)
  }
}

case class GenericStreamDataSourceConfiguration(
  format: FormatType,
  options: Map[String, String],
  schema: Option[StructType]
) extends FormatAwareStreamingSourceConfiguration {
  override def addOptions(extraOptions: Map[String, String]): GenericStreamDataSourceConfiguration =
    this.copy(options = this.options ++ extraOptions)
  override def withSchema(schema: Option[StructType]): GenericStreamDataSourceConfiguration =
    this.copy(schema = schema)
}
object GenericStreamDataSourceConfiguration {
  def apply(
    format: FormatType,
    options: Option[Map[String, String]] = None,
    schema: Option[StructType] = None
  ): GenericStreamDataSourceConfiguration =
    GenericStreamDataSourceConfiguration(format, options.getOrElse(Map()), schema)
}
