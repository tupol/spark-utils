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
import org.tupol.utils.implicits._

import scala.util.Try

/**  GenericDataSink trait */
case class GenericDataSink(configuration: GenericSinkConfiguration) extends DataSink[GenericSinkConfiguration, DataFrame] with Logging {

  /** Configure a `writer` for the given `DataFrame` based on the given `GenericSinkConfiguration` */
  private def configureWriter(data: DataFrame, configuration: GenericSinkConfiguration): DataFrameWriter[Row] = {
    val basicWriter = data.write
      .format(configuration.format.toString)
      .mode(configuration.saveMode)
    val writerWithPartitions = configuration.partition match {
      case Some(partitions) => basicWriter.partitionBy(partitions.columns: _*)
      case None => basicWriter
    }
    val writerWithOptions = configuration.options match {
      case Some(options) => writerWithPartitions.options(options)
      case None => writerWithPartitions
    }
    writerWithOptions
  }

  /** Try to write the data according to the given configuration and return the same data or a failure */
  override def write(data: DataFrame): Try[DataFrame] = {
    logInfo(s"Writing data as '${configuration.format}' to '${configuration}'.")
    Try(configureWriter(data, configuration).save())
      .map(_ => data)
      .mapFailure(DataSinkException(s"Failed to save the data as '${configuration.format}' to '${configuration}').", _))
      .logSuccess(_ => logInfo(s"Successfully saved the data as '${configuration.format}' to '${configuration}'."))
      .logFailure(logError)
  }
}

/** GenericDataSink trait that is data aware, so it can perform a write call with no arguments */
case class GenericDataAwareSink(configuration: GenericSinkConfiguration, data: DataFrame) extends DataAwareSink[GenericSinkConfiguration, DataFrame] {
  override def sink: DataSink[GenericSinkConfiguration, DataFrame] = GenericDataSink(configuration)
}

/**
 * Output DataFrame sink configuration for Hadoop files.
 * @param format the format can be `csv`, `json`, `orc`, `parquet`, `com.databricks.spark.avro` or just `avro` and
 *               `com.databricks.spark.xml` or just `xml`
 * @param optionalSaveMode the save mode can be `overwrite`, `append`, `ignore` and `error`;
 *                         more details available at [[https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/GenericDataSink.html#mode-java.lang.String-]]
 * @param partitionColumns optionally the writer can layout data in partitions similar to the hive partitions
 * @param buckets optionally the writer can bucket the data, similar to Hive bucketing
 * @param options other sink specific options
 *
 */
case class GenericSinkConfiguration(format: FormatType, mode: Option[String], partition: Option[PartitionsConfiguration],
  buckets: Option[BucketsConfiguration],
  options: Option[Map[String, String]])
  extends FormatAwareDataSinkConfiguration {
  def optionalSaveMode: Option[String] = mode
  def saveMode = optionalSaveMode.getOrElse("default")
  override def toString: String = {
    val optionsStr = options match {
      case Some(options) => if(options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
      case None => ""
    }
    s"format: '$format', save mode: '$saveMode', " +
      s"partitioning: [${partition.getOrElse("None")}], " +
      s"bucketing: ${buckets.getOrElse("None")}, " +
      s"options: {$optionsStr}"
  }
}

object GenericSinkConfiguration {
  def apply(format: FormatType, mode: Option[String] = None, partitionColumns: Seq[String] = Seq(),
            buckets: Option[BucketsConfiguration] = None,
            options: Map[String, String] = Map()): GenericSinkConfiguration = {
    val partitions = partitionColumns match {
      case Nil => None
      case _ => Some(PartitionsConfiguration(None, partitionColumns))
    }
    val optionalOptions = options.toSeq match {
      case Nil => None
      case _ => Some(options)
    }
    new GenericSinkConfiguration(format, mode, partitions, buckets, optionalOptions)
  }
}