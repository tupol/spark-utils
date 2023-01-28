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

import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row}
import org.tupol.spark.Logging
import org.tupol.spark.io.{DataAwareSink, DataSink, DataSinkException, FormatType, PartitionsConfiguration}
import org.tupol.utils.implicits._
import org.tupol.utils.CollectionOps._

import scala.util.Try

case class GenericStreamDataSink(configuration: GenericStreamDataSinkConfiguration)
  extends DataSink[GenericStreamDataSinkConfiguration, DataStreamWriter[Row], StreamingQuery] with Logging {

  def writer(data: DataFrame): Try[DataStreamWriter[Row]] = Try {
    val basicWriter = data.writeStream
      .format(configuration.format.toString)
      .options(configuration.options.getOrElse(Map()))
    val writerWithOutputMode = configuration.outputMode.map(basicWriter.outputMode(_)).getOrElse(basicWriter)
    val writerWithQueryName = configuration.queryName.map(writerWithOutputMode.queryName(_)).getOrElse(writerWithOutputMode)
    val writerWithTrigger = configuration.trigger.map(writerWithQueryName.trigger(_)).getOrElse(writerWithQueryName)
    val writerWithPartitions = configuration.partition match {
      case None => writerWithTrigger
      case Some(PartitionsConfiguration(_, Nil)) => writerWithTrigger
      case Some(PartitionsConfiguration(_, partitionColumns)) => writerWithTrigger.partitionBy(partitionColumns: _*)
    }
    writerWithPartitions
  }

  /** Try to write the data according to the given configuration and return the same data or a failure */
  override def write(data: DataFrame): Try[StreamingQuery] = {
    logInfo(s"Writing data to { $configuration }.")
    Try(writer(data).map(_.start())).flatten
      .mapFailure(DataSinkException(s"Failed writing the data to { $configuration }.", _))
      .logSuccess(_ => logInfo(s"Successfully writing the data to { $configuration }."))
      .logFailure(logError)
  }
}

/** GenericStreamDataAwareSink is "data aware", so it can perform a write call with no arguments */
case class GenericStreamDataAwareSink(configuration: GenericStreamDataSinkConfiguration, data: DataFrame)
  extends DataAwareSink[GenericStreamDataSinkConfiguration, DataStreamWriter[Row], StreamingQuery] {
  override def sink: DataSink[GenericStreamDataSinkConfiguration, DataStreamWriter[Row], StreamingQuery] = GenericStreamDataSink(configuration)
}

case class GenericStreamDataSinkConfiguration(format: FormatType, options: Option[Map[String, String]],
                                              queryName: Option[String], trigger: Option[Trigger],
                                              partition: Option[PartitionsConfiguration], outputMode: Option[String])
  extends FormatAwareStreamingSinkConfiguration {
  def addOptions(options: Map[String, String]): GenericStreamDataSinkConfiguration = {
    val newOptions = Option(this.options.map(_ ++ options).getOrElse(options))
    this.copy(options = newOptions)
  }
  override def toString: String = {
    val optionsStr = options.map(options => if (options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")).getOrElse("")
    val partitionColsStr = partition.map(_.columns.mkString(", ")).getOrElse("")
    s"format: '$format', " +
      s"partition columns: [$partitionColsStr], " +
      s"options: {$optionsStr}, " +
      s"query name: ${queryName.getOrElse("not specified")}, " +
      s"output mode: ${outputMode.getOrElse("not specified")}, " +
      s"trigger: ${trigger.getOrElse("not specified")}"
  }
}
object GenericStreamDataSinkConfiguration {
  def apply(format: FormatType, options: Map[String, String] = Map(),
            queryName: Option[String] = None, trigger: Option[Trigger] = None,
            partitionColumns: Seq[String] = Seq(), outputMode: Option[String] = None): GenericStreamDataSinkConfiguration = {
    val partition = partitionColumns.toOptionNel.map(PartitionsConfiguration(None, _))
    GenericStreamDataSinkConfiguration(format, options.toSeq.toOptionNel.map(_.toMap), queryName, trigger, partition, outputMode)
  }

}


