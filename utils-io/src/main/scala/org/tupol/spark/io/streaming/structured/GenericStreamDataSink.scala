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

import org.apache.spark.sql.streaming.{ DataStreamWriter, StreamingQuery, Trigger }
import org.apache.spark.sql.{ DataFrame, Row }
import org.tupol.spark.Logging
import org.tupol.spark.io.{ DataAwareSink, DataSink, DataSinkException, FormatType }
import org.tupol.configz.Configurator
import org.tupol.utils.implicits._
import scalaz.ValidationNel

import scala.util.Try

case class GenericStreamDataSink(configuration: GenericStreamDataSinkConfiguration)
  extends DataSink[GenericStreamDataSinkConfiguration, StreamingQuery] with Logging {

  private def configureWriter(data: DataFrame, configuration: GenericStreamDataSinkConfiguration): DataStreamWriter[Row] = {
    val basicWriter = data.writeStream
      .format(configuration.format.toString)
      .options(configuration.options)
    val writerWithOutputMode = configuration.outputMode.map(basicWriter.outputMode(_)).getOrElse(basicWriter)
    val writerWithQueryName = configuration.queryName.map(writerWithOutputMode.queryName(_)).getOrElse(writerWithOutputMode)
    val writerWithTrigger = configuration.trigger.map(writerWithQueryName.trigger(_)).getOrElse(writerWithQueryName)
    val writerWithPartitions = configuration.partitionColumns match {
      case Nil => writerWithTrigger
      case _ => writerWithTrigger.partitionBy(configuration.partitionColumns: _*)
    }
    writerWithPartitions
  }

  /** Try to write the data according to the given configuration and return the same data or a failure */
  override def write(data: DataFrame): Try[StreamingQuery] = {
    logInfo(s"Writing data to { $configuration }.")
    Try(configureWriter(data, configuration).start())
      .mapFailure(DataSinkException(s"Failed writing the data to { $configuration }.", _))
      .logSuccess(_ => logInfo(s"Successfully writing the data to { $configuration }."))
      .logFailure(logError)
  }
}

/** GenericStreamDataAwareSink is "data aware", so it can perform a write call with no arguments */
case class GenericStreamDataAwareSink(configuration: GenericStreamDataSinkConfiguration, data: DataFrame)
  extends DataAwareSink[GenericStreamDataSinkConfiguration, StreamingQuery] {
  override def sink: DataSink[GenericStreamDataSinkConfiguration, StreamingQuery] = GenericStreamDataSink(configuration)
}

case class GenericStreamDataSinkConfiguration(format: FormatType, options: Map[String, String] = Map(),
  queryName: Option[String] = None, trigger: Option[Trigger] = None,
  partitionColumns: Seq[String] = Seq(), outputMode: Option[String] = None)
  extends FormatAwareStreamingSinkConfiguration {
  override def toString: String = {
    val optionsStr = if (options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
    s"format: '$format', " +
      s"partition columns: [${partitionColumns.mkString(", ")}], " +
      s"options: {$optionsStr}, " +
      s"query name: ${queryName.getOrElse("not specified")}, " +
      s"output mode: ${outputMode.getOrElse("not specified")}, " +
      s"trigger: ${trigger.getOrElse("not specified")}"
  }
}

object GenericStreamDataSinkConfiguration extends Configurator[GenericStreamDataSinkConfiguration] {
  import com.typesafe.config.Config
  import org.tupol.configz._
  import scalaz.syntax.applicative._

  def validationNel(config: Config): ValidationNel[Throwable, GenericStreamDataSinkConfiguration] = {
    config.extract[FormatType]("format") |@|
      config.extract[Option[Map[String, String]]]("options").map(_.getOrElse(Map())) |@|
      config.extract[Option[String]]("queryName") |@|
      config.extract[Option[Trigger]] |@|
      config.extract[Option[Seq[String]]]("partition.columns").map {
        case (Some(partition_columns)) => partition_columns
        case None => Seq[String]()
      } |@|
      config.extract[Option[String]]("outputMode") apply
      GenericStreamDataSinkConfiguration.apply
  }
}
