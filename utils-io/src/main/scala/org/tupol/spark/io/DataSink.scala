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

import org.apache.spark.sql.DataFrame

import scala.util.Try

/** Common trait for writing a DataFrame to an external resource */
trait DataSink[Config <: DataSinkConfiguration, Writer, WriteOut] {
  def configuration: Config
  def writer(data: DataFrame): Try[Writer]
  def write(data: DataFrame): Try[WriteOut]
}

/** Common trait for writing an already defined data DataFrame to an external resource */
trait DataAwareSink[Config <: DataSinkConfiguration, Writer, WriteOut] extends DataSink[Config, Writer, WriteOut] {
  def data: DataFrame
  def sink: DataSink[Config, Writer, WriteOut]
  def writer(data: DataFrame): Try[Writer]  = sink.writer(data)
  def writer: Try[Writer]                   = writer(data)
  def write(data: DataFrame): Try[WriteOut] = sink.write(data)
  def write: Try[WriteOut]                  = write(data)
}

/** Factory trait for DataAwareSinkFactory */
trait DataAwareSinkFactory {
  def apply[Config <: DataSinkConfiguration, Writer, WriteOut](
    configuration: Config,
    data: DataFrame
  ): DataAwareSink[Config, Writer, WriteOut]
}

/** Common marker trait for `DataSink` configuration that also knows the data format */
trait FormatAwareDataSinkConfiguration extends DataSinkConfiguration with FormatAware

/** Common marker trait for `DataSink` configuration */
trait DataSinkConfiguration

case class DataSinkException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
