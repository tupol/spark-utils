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

/**  FileDataSink trait */
case class FileDataSink(configuration: FileSinkConfiguration) extends DataSink[FileSinkConfiguration, DataFrame] with Logging {

  /**
   * Configure a `writer` for the given `DataFrame` using the given `FileSinkConfiguration`,
   * setting up the partitions, partitions number, save mode and format
   */
  private def configureWriter(data: DataFrame, configuration: FileSinkConfiguration): DataFrameWriter[Row] = {
    val writer = configuration.partition.flatMap(_.number) match {
      case Some(partsNo) =>
        logDebug(s"Initializing the DataFrameWriter after repartitioning data to $partsNo partitions.")
        // For the distinction between repartition and coalesce please check the API
        if (partsNo <= 2) data.repartition(partsNo).write
        else data.coalesce(partsNo).write
      case None => data.write
    }
    val partitionsWriter = configuration.partition match {
      case None => writer
      case Some(PartitionsConfiguration(_, Nil)) => writer
      case Some(PartitionsConfiguration(_, columns)) =>
        logDebug(s"Initializing the DataFrameWriter to partition the data using the following partition columns: " +
          s"[${columns.mkString(", ")}].")
        writer.partitionBy(columns: _*)
    }
    val bucketsWriter = configuration.buckets match {
      case None => partitionsWriter
      case Some(bc) =>
        logDebug(s"Initializing the DataFrameWriter to bucket the data into ${bc.number} buckets " +
          s"using the following partition columns: ${bc.columns.mkString(", ")}].")
        val sortedWriter = bc.sortByColumns match {
          case None => partitionsWriter
          case Some(Nil) => partitionsWriter
          case Some(sortByColumns) =>
            logDebug(s"Buckets will be sorted by the following columns: ${sortByColumns.mkString(", ")}].")
            partitionsWriter.sortBy(sortByColumns.head, sortByColumns.tail: _*)
        }
        sortedWriter.bucketBy(bc.number, bc.columns.head, bc.columns.tail: _*)
    }
    configuration.options match {
      case None => bucketsWriter.mode(configuration.saveMode).format(configuration.format.toString)
      case Some(options) => bucketsWriter.mode(configuration.saveMode).format(configuration.format.toString).options(options)
    }

  }

  /** Try to write the data according to the given configuration and return the same data or a failure */
  def write(data: DataFrame): Try[DataFrame] = {

    val writer = configureWriter(data, configuration)
    Try {
      configuration.buckets match {
        case Some(bc) =>
          logInfo(s"Writing data to Hive as '${configuration.format}' in the '${configuration.path}' table due to the buckets configuration: $bc. " +
            s"Notice that the path parameter is used as a table name in this case.")
          writer.saveAsTable(configuration.path)
        case None =>
          logInfo(s"Writing data as '${configuration.format}' to '${configuration.path}'.")
          writer.save(configuration.path)
      }
    }
      .map(_ => data)
      .logSuccess(_ => logInfo(s"Successfully saved the data as '${configuration.format}' to '${configuration.path}' " +
        s"(Full configuration: ${configuration})."))
      .mapFailure(DataSinkException(s"Failed to save the data as '${configuration.format}' to '${configuration.path}' " +
        s"(Full configuration: ${configuration}).", _))
      .logFailure(logError)
  }
}

/** FileDataSink trait that is data aware, so it can perform a write call with no arguments */
case class FileDataAwareSink(configuration: FileSinkConfiguration, data: DataFrame) extends DataAwareSink[FileSinkConfiguration, DataFrame] {
  override def sink: DataSink[FileSinkConfiguration, DataFrame] = FileDataSink(configuration)
}

/**
 * Output DataFrame sink configuration for Hadoop files.
 * @param path the path of the target file
 * @param format the format can be `csv`, `json`, `orc`, `parquet`, `com.databricks.spark.avro` or just `avro` and
 *               `com.databricks.spark.xml` or just `xml`
 * @param optionalSaveMode the save mode can be `overwrite`, `append`, `ignore` and `error`;
 *                         more details available at [[https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/FileDataSink.html#mode-java.lang.String-]]
 * @param partitionFilesNumber the number of partitions that the data will be partitioned to;
 *                         if not given the number of partitions will be left unchanged
 * @param partitionColumns optionally the writer can layout data in partitions similar to the hive partitions
 * @param buckets optionally the writer can bucket the data, similar to Hive bucketing
 * @param options other sink specific options
 *
 */
case class FileSinkConfiguration(path: String, format: FormatType, mode: Option[String],
  partition: Option[PartitionsConfiguration],
  buckets: Option[BucketsConfiguration],
  options: Option[Map[String, String]])
  extends FormatAwareDataSinkConfiguration {
  def saveMode = mode.getOrElse("default")
  override def toString: String = {
    val optionsStr = options match {
      case Some(options) => if(options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
      case None => ""
    }
    s"path: '$path', format: '$format', save mode: '$saveMode', " +
      s"partitioning: ${partition.getOrElse("None")}, " +
      s"bucketing: ${buckets.getOrElse("None")}, " +
      s"options: {$optionsStr}"
  }
}
object FileSinkConfiguration {
  def apply(path: String, format: FormatType, optionalSaveMode: Option[String] = None,
            partitionFilesNumber: Option[Int] = None, partitionColumns: Seq[String] = Seq(),
            buckets: Option[BucketsConfiguration] = None,
            options: Option[Map[String, String]] = None): FileSinkConfiguration = {
    val partition = (partitionFilesNumber, partitionColumns) match {
      case (None, Nil) => None
      case (None, columns) => Some(PartitionsConfiguration(None, columns))
      case (number, columns) => Some(PartitionsConfiguration(number, columns))
    }
    new FileSinkConfiguration(path, format, optionalSaveMode, partition, buckets, options)
  }
}

/**
 * Partitioning configuration
 * @param number number of partition files
 * @param columns optional columns for partitioning
 */
case class PartitionsConfiguration(number: Option[Int], columns: Seq[String]) {
  override def toString: String = {
    s"number of partition: '${number.getOrElse("Unchanged")}', " +
      s"partition columns: [${columns.mkString(", ")}]"
  }
}

/**
 * Bucketing configuration
 * @param number number of buckets
 * @param columns columns for bucketing
 * @param sortByColumns optional sort columns for bucketing
 */
case class BucketsConfiguration(number: Int, columns: Seq[String], sortByColumns: Option[Seq[String]]) {
  override def toString: String = {
    val sortByColumnsStr = sortByColumns match {
      case None => "not specified"
      case Some(sbc) => s"[${sbc.mkString(", ")}]"
    }
    s"number of buckets: '$number', " +
      s"bucketing columns: [${columns.mkString(", ")}], " +
      s"sortBy columns: $sortByColumnsStr"
  }
}

object BucketsConfiguration {
  def apply(number: Int, columns: Seq[String], sortByColumns: Seq[String]): BucketsConfiguration = {
    val optSortByColumns = sortByColumns match {
      case Nil => None
      case _ => Some(sortByColumns)
    }
    new BucketsConfiguration(number, columns, optSortByColumns)
  }
}

