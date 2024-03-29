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

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.streaming.{ DataStreamWriter, StreamingQuery }
import org.tupol.spark.Logging
import org.tupol.spark.io.{ DataAwareSink, DataSink, FormatType }

import scala.util.Try

case class FileStreamDataSink(configuration: FileStreamDataSinkConfiguration)
    extends DataSink[FileStreamDataSinkConfiguration, DataStreamWriter[Row], StreamingQuery]
    with Logging {

  private val innerSink = GenericStreamDataSink(configuration.generic)

  override def writer(data: DataFrame): Try[DataStreamWriter[Row]] = innerSink.writer(data)

  /** Try to write the data according to the given configuration and return the same data or a failure */
  override def write(data: DataFrame): Try[StreamingQuery] = innerSink.write(data)

}

object FileStreamDataSink {
  def apply(
    path: String,
    genericConfig: GenericStreamDataSinkConfiguration,
    checkpointLocation: Option[String] = None
  ): FileStreamDataSinkConfiguration =
    FileStreamDataSinkConfiguration(path, genericConfig, checkpointLocation)
}

/** FileDataSink trait that is data aware, so it can perform a write call with no arguments */
case class FileStreamDataAwareSink(configuration: FileStreamDataSinkConfiguration, data: DataFrame)
    extends DataAwareSink[FileStreamDataSinkConfiguration, DataStreamWriter[Row], StreamingQuery] {
  override def sink: DataSink[FileStreamDataSinkConfiguration, DataStreamWriter[Row], StreamingQuery] =
    FileStreamDataSink(configuration)
}

case class FileStreamDataSinkConfiguration(
  path: String,
  private val genericConfig: GenericStreamDataSinkConfiguration,
  checkpointLocation: Option[String]
) extends FormatAwareStreamingSinkConfiguration {
  def addOptions(extraOptions: Map[String, String]): FileStreamDataSinkConfiguration =
    this.copy(genericConfig = genericConfig.addOptions(internalOptions ++ extraOptions))
  private val internalOptions =
    Map("path" -> Some(path), "checkpointLocation" -> checkpointLocation).collect {
      case (key, Some(value)) => (key, value)
    }
  def options(): Map[String, String] = genericConfig.options.getOrElse(Map())

  /** The generic configuration of this data sink; this is used to build the actual writer */
  val generic                                  = genericConfig.addOptions(internalOptions)
  def format: FormatType                       = generic.format
  def resolve: FileStreamDataSinkConfiguration = this.copy(genericConfig = generic)

  override def toString: String = s"path: '$path', $generic"
}
