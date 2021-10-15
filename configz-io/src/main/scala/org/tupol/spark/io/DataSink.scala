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
import org.apache.spark.sql.DataFrame
import org.tupol.configz.Configurator
import scalaz.{ NonEmptyList, ValidationNel }

import scala.util.Try

/** Factory for DataSourceConfiguration */
object FormatAwareDataSinkConfiguration extends Configurator[FormatAwareDataSinkConfiguration] {
  override def validationNel(config: Config): ValidationNel[Throwable, FormatAwareDataSinkConfiguration] = {
    import org.tupol.configz._
    val format = config.extract[FormatType]("format")
    format match {
      case scalaz.Success(formatString) =>
        formatString match {
          case FormatType.Jdbc => JdbcSinkConfiguration.validationNel(config)
          case f if (FormatType.AcceptableFileFormats.contains(f)) =>
            FileSinkConfiguration.validationNel(config)
          case _ => GenericSinkConfiguration.validationNel(config)
        }
      case scalaz.Failure(e) =>
        scalaz.Failure[NonEmptyList[Throwable]](e)
    }
  }
}

/** Common marker trait for `DataSink` configuration */
trait DataSinkConfiguration

/** Factory for DataSourceConfiguration */
object DataSinkConfiguration extends Configurator[DataSinkConfiguration] {
  override def validationNel(config: Config): ValidationNel[Throwable, DataSinkConfiguration] =
    FormatAwareDataSinkConfiguration.validationNel(config)
}
