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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.tupol.spark.Logging
import org.tupol.spark.io.FormatType._
import org.tupol.spark.io.{ DataSource, FormatType, _ }
import org.tupol.configz.Configurator
import scalaz.{ NonEmptyList, ValidationNel }

import scala.util.Try

object KafkaStreamDataSourceConfiguration extends Configurator[KafkaStreamDataSourceConfiguration] {
  val AcceptableFormat = Kafka
  override def validationNel(config: Config): ValidationNel[Throwable, KafkaStreamDataSourceConfiguration] = {
    import org.tupol.configz._
    import scalaz.syntax.applicative._

    val format = config.extract[Option[FormatType]]("format").ensure(
      new IllegalArgumentException(s"This is a Kafka Data Source, only the $Kafka format is supported.").toNel)(f =>
        f.map(_ == Kafka).getOrElse(true))

    format match {
      case scalaz.Success(_) =>
        config.extract[String]("kafka.bootstrap.servers") |@|
          config.extract[KafkaSubscription] |@|
          config.extract[Option[String]]("startingOffsets") |@|
          config.extract[Option[String]]("endingOffsets") |@|
          config.extract[Option[Boolean]]("failOnDataLoss") |@|
          config.extract[Option[Long]]("kafkaConsumer.pollTimeoutMs") |@|
          config.extract[Option[Int]]("fetchOffset.numRetries") |@|
          config.extract[Option[Long]]("fetchOffset.retryIntervalMs") |@|
          config.extract[Option[Long]]("maxOffsetsPerTrigger") |@|
          config.extract[Option[StructType]]("schema") apply
          KafkaStreamDataSourceConfiguration.apply
      case scalaz.Failure(e) =>
        scalaz.Failure[NonEmptyList[Throwable]](e)
    }
  }
}
