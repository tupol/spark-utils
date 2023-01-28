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
package org.tupol.spark.io.configz

import org.tupol.configz.Configurator
import org.tupol.spark.Logging
import org.tupol.spark.io._

object GenericSinkConfigurator extends Configurator[GenericSinkConfiguration] with Logging {
  import com.typesafe.config.Config
  import org.tupol.configz._
  import scalaz.ValidationNel
  import scalaz.syntax.applicative._

  implicit val bucketsExtractor = BucketsConfigurator

  def validationNel(config: Config): ValidationNel[Throwable, GenericSinkConfiguration] = {
    config.extract[FormatType]("format") |@|
      config.extract[Option[String]]("mode") |@|
      config.extract[Option[Seq[String]]]("partition.columns").map {
        case (Some(partition_columns)) => partition_columns
        case None => Seq[String]()
      } |@|
      config.extract[Option[BucketsConfiguration]]("buckets") |@|
      config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map[String, String]())) apply
      GenericSinkConfiguration.apply
  }
}
