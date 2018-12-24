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
import org.tupol.utils.config.Configurator

import scala.util.{ Failure, Success, Try }

case class FileDataFrameSaver(configuration: FileDataFrameSaverConfig) extends Logging {

  /**
   * Configure a `writer` for the given `DataFrame` using the `config`, setting up the partitions, partitions number,
   * save mode and format
   * @param data `DataFrame` for which we are configuring the FileDataFrameSaver
   * @param config the configuration that needs to be applied to the `writer`
   * @return
   */
  private def configureWriter(data: DataFrame, config: FileDataFrameSaverConfig): DataFrameWriter[Row] = {
    val writer = config.partitionFilesNumber match {
      case Some(partsNo) =>
        logDebug(s"Initializing the DataFrameWriter after repartitioning data to $partsNo partitions.")
        // For the distinction between repartition and coalesce please check the API
        if (partsNo <= 2) data.repartition(partsNo).write
        else data.coalesce(partsNo).write
      case None => data.write
    }
    val partitionsWriter = config.partitionColumns match {
      case Nil => writer
      case partitions =>
        logDebug(s"Initializing the DataFrameWriter to partition the data using the following partition columns: " +
          s"${partitions.mkString("'", "', '", "'")}.")
        writer.partitionBy(partitions: _*)
    }
    partitionsWriter.mode(config.saveMode).format(config.format.toString)
  }

  /**
   * Try to save the data according to the given configuration and return the same data or a failure
   * @param data
   * @return Try[DataFrame]
   */
  def saveData(data: DataFrame): Try[DataFrame] = {
    logInfo(s"Writing data to '${configuration.path}' as '${configuration.format}'.")
    Try(configureWriter(data, configuration).save(configuration.path)) match {
      case Success(_) =>
        logInfo(s"Successfully saved the data to '${configuration.path}' as '${configuration.format}' " +
          s"(Full Configuration: ${configuration}).")
        Success(data)
      case Failure(ex) =>
        logError(s"Failed to save the data to '${configuration.path}' as '${configuration.format}' " +
          s"(Full Configuration: ${configuration}).")
        Failure(ex)
    }
  }
}

/**
 * Output DataFrame saver configuration for Hadoop files.
 * @param path the path of the target file
 * @param format the format can be `csv`, `json`, `orc`, `parquet`, `com.databricks.spark.avro` or just `avro` and
 *               `com.databricks.spark.xml` or just `xml`
 * @param optionalSaveMode the save mode can be `overwrite`, `append`, `ignore` and `error`;
 *                         more details available at [[https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/FileDataFrameSaver.html#mode-java.lang.String-]]
 * @param partitionFilesNumber the number of partitions that the data will be partitioned to;
 *                         if not given the number of partitions will be left unchanged
 * @param partitionColumns optionally the writer can layout data in partitions similar to the hive partitions
 *
 * TODO: Move this to the `sdp-commons` project.
 */
case class FileDataFrameSaverConfig(path: String, format: FormatType, optionalSaveMode: Option[String] = None,
    partitionFilesNumber: Option[Int] = None, partitionColumns: Seq[String] = Seq()) {
  def saveMode = optionalSaveMode.getOrElse("default")
  override def toString: String = s"path: '$path', format: '$format', optionalSaveMode: '$optionalSaveMode', " +
    s"partitionsNumber: $partitionFilesNumber, partitionColumns: [${partitionColumns.mkString("'", ", ", "'")}]"
}

object FileDataFrameSaverConfig extends Configurator[FileDataFrameSaverConfig] with Logging {
  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel

  def apply(path: String, format: FormatType): FileDataFrameSaverConfig = new FileDataFrameSaverConfig(path, format, None, None, Seq())

  def validationNel(config: Config): ValidationNel[Throwable, FileDataFrameSaverConfig] = {
    import scalaz.syntax.applicative._
    config.extract[String]("path") |@|
      config.extract[FormatType]("format") |@|
      config.extract[Option[String]]("mode") |@|
      config.extract[Option[Int]]("partition.files").map { partFiles =>
        partFiles.flatMap(pf => if (pf <= 0) None else Some(pf))
      } |@|
      config.extract[Option[Seq[String]]]("partition.columns").map {
        case (Some(partition_columns)) => partition_columns
        case None => Seq[String]()
      } apply
      FileDataFrameSaverConfig.apply
  }
}
