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

import org.apache.spark.sql.{ DataFrame, DataFrameWriter, Row }
import org.tupol.spark.Logging
import org.tupol.configz.Configurator
import org.tupol.utils.implicits._

import scala.util.Try

object FileSinkConfiguration extends Configurator[FileSinkConfiguration] with Logging {
  import com.typesafe.config.Config
  import org.tupol.configz._
  import scalaz.ValidationNel
  import scalaz.syntax.applicative._

  implicit val bucketsExtractor = BucketsConfiguration

  def apply(path: String, format: FormatType): FileSinkConfiguration = new FileSinkConfiguration(path, format, None, None, Seq())

  def validationNel(config: Config): ValidationNel[Throwable, FileSinkConfiguration] = {
    config.extract[String]("path") |@|
      config.extract[FormatType]("format").ensure(
        new IllegalArgumentException(s"The provided format is unsupported for a file data sink. " +
          s"Supported formats are: ${FormatType.AcceptableFileFormats.mkString("'", "', '", "'")}").toNel)(f => FormatType.AcceptableFileFormats.contains(f)) |@|
        config.extract[Option[String]]("mode") |@|
        config.extract[Option[Int]]("partition.files").
        ensure(new IllegalArgumentException(
          "If specified, the partition.files should be a positive integer > 0.").toNel)(_.map(_ > 0).getOrElse(true)) |@|
        config.extract[Option[Seq[String]]]("partition.columns").map {
          case (Some(partition_columns)) => partition_columns
          case None => Seq[String]()
        } |@|
        config.extract[Option[BucketsConfiguration]]("buckets") |@|
        config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]())) apply
        FileSinkConfiguration.apply
  }
}

object BucketsConfiguration extends Configurator[BucketsConfiguration] {
  import com.typesafe.config.Config
  import org.tupol.configz._
  import scalaz.ValidationNel
  import scalaz.syntax.applicative._

  def validationNel(config: Config): ValidationNel[Throwable, BucketsConfiguration] = {
    config.extract[Int]("number")
      .ensure(new IllegalArgumentException("The number of buckets must be a positive integer > 0.").toNel)(_ > 0) |@|
      config.extract[Seq[String]]("bucketColumns")
      .ensure(new IllegalArgumentException("At least one column needs to be specified for bucketing.").toNel)(_.size > 0) |@|
      config.extract[Option[Seq[String]]]("sortByColumns").map {
        case (Some(sortByColumns)) => sortByColumns
        case None => Seq[String]()
      } apply BucketsConfiguration.apply
  }
}
