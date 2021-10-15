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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.tupol.spark.Logging
import org.tupol.spark.io.{ DataAwareSink, DataSink, FormatType }
import org.tupol.configz.Configurator
import scalaz.{ NonEmptyList, ValidationNel }

import scala.util.Try

object FileStreamDataSinkConfiguration extends Configurator[FileStreamDataSinkConfiguration] {
  import com.typesafe.config.Config
  import org.tupol.configz._
  import scalaz.syntax.applicative._

  def apply(
    path: String,
    genericConfig: GenericStreamDataSinkConfiguration,
    checkpointLocation: Option[String] = None): FileStreamDataSinkConfiguration =
    new FileStreamDataSinkConfiguration(genericConfig.format, path, genericConfig, checkpointLocation)

  def validationNel(config: Config): ValidationNel[Throwable, FileStreamDataSinkConfiguration] = {

    val format = config.extract[FormatType]("format").ensure(
      new IllegalArgumentException(s"The provided format is unsupported for a file data source. " +
        s"Supported formats are: ${FormatType.AcceptableFileFormats.mkString("'", "', '", "'")}").toNel)(f =>
        FormatType.AcceptableFileFormats.contains(f))

    format match {
      case scalaz.Success(_) =>
        format |@|
          config.extract[String]("path") |@|
          GenericStreamDataSinkConfiguration.validationNel(config) |@|
          config.extract[Option[String]]("checkpointLocation") apply
          FileStreamDataSinkConfiguration.apply
      case scalaz.Failure(e) =>
        scalaz.Failure[NonEmptyList[Throwable]](e)
    }
  }
}
