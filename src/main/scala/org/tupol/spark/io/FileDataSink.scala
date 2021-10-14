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
import org.tupol.utils.configz.Configurator
import org.tupol.utils.implicits._

import scala.util.Try

/**  FileDataSink trait */
case class FileDataSink(configuration: FileSinkConfiguration) extends DataSink[FileSinkConfiguration, DataFrame] with Logging {

  /**
   * Configure a `writer` for the given `DataFrame` using the given `FileSinkConfiguration`,
   * setting up the partitions, partitions number, save mode and format
   */
  private def configureWriter(data: DataFrame, configuration: FileSinkConfiguration): DataFrameWriter[Row] = {
    val writer = configuration.partitionFilesNumber match {
      case Some(partsNo) =>
        logDebug(s"Initializing the DataFrameWriter after repartitioning data to $partsNo partitions.")
        // For the distinction between repartition and coalesce please check the API
        if (partsNo <= 2) data.repartition(partsNo).write
        else data.coalesce(partsNo).write
      case None => data.write
    }
    val partitionsWriter = configuration.partitionColumns match {
      case Nil => writer
      case partitions =>
        logDebug(s"Initializing the DataFrameWriter to partition the data using the following partition columns: " +
          s"[${partitions.mkString(", ")}].")
        writer.partitionBy(partitions: _*)
    }
    val bucketsWriter = configuration.buckets match {
      case None => partitionsWriter
      case Some(bc) =>
        logDebug(s"Initializing the DataFrameWriter to bucket the data into ${bc.number} buckets " +
          s"using the following partition columns: ${bc.bucketColumns.mkString(", ")}].")
        val sortedWriter = bc.sortByColumns match {
          case Nil => partitionsWriter
          case _ =>
            logDebug(s"Buckets will be sorted by the following columns: ${bc.sortByColumns.mkString(", ")}].")
            partitionsWriter.sortBy(bc.sortByColumns.head, bc.sortByColumns.tail: _*)
        }
        sortedWriter.bucketBy(bc.number, bc.bucketColumns.head, bc.bucketColumns.tail: _*)
    }
    bucketsWriter.mode(configuration.saveMode).format(configuration.format.toString).options(configuration.options)
  }

  /** Try to write the data according to the given configuration and return the same data or a failure */
  def write(data: DataFrame): Try[DataFrame] = {

    Try {
      configuration.buckets match {
        case Some(bc) =>
          logInfo(s"Writing data to Hive as '${configuration.format}' in the '${configuration.path}' table. " +
            s"Notice that the path parameter is used as a table name in this case.")
          configureWriter(data, configuration).saveAsTable(configuration.path)
        case None =>
          logInfo(s"Writing data as '${configuration.format}' to '${configuration.path}'.")
          configureWriter(data, configuration).save(configuration.path)
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
case class FileSinkConfiguration(path: String, format: FormatType, optionalSaveMode: Option[String] = None,
  partitionFilesNumber: Option[Int] = None, partitionColumns: Seq[String] = Seq(),
  buckets: Option[BucketsConfiguration] = None,
  options: Map[String, String] = Map())
  extends FormatAwareDataSinkConfiguration {
  def saveMode = optionalSaveMode.getOrElse("default")
  override def toString: String = {
    val optionsStr = if (options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
    s"path: '$path', format: '$format', save mode: '$saveMode', " +
      s"partition files number: ${partitionFilesNumber.getOrElse("not specified")}, " +
      s"partition columns: [${partitionColumns.mkString(", ")}], " +
      s"bucketing: ${buckets.getOrElse("None")}, " +
      s"options: {$optionsStr}"
  }
}

object FileSinkConfiguration extends Configurator[FileSinkConfiguration] with Logging {
  import com.typesafe.config.Config
  import org.tupol.utils.configz._
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

/**
 * Bucketing configuration
 * @param number number of buckets
 * @param bucketColumns columns for bucketing
 * @param sortByColumns optional sort columns for bucketing
 */
case class BucketsConfiguration(number: Int, bucketColumns: Seq[String], sortByColumns: Seq[String] = Seq()) {
  override def toString: String = {
    s"number of buckets: '$number', " +
      s"bucketing columns: [${bucketColumns.mkString(", ")}], " +
      s"sortBy columns: [${sortByColumns.mkString(", ")}]"
  }
}

object BucketsConfiguration extends Configurator[BucketsConfiguration] {
  import com.typesafe.config.Config
  import org.tupol.utils.configz._
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
