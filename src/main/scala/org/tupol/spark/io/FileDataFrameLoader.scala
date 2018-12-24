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
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SparkSession }
import org.tupol.spark.Logging
import org.tupol.spark.io.parsers.{ ColumnNameOfCorruptRecord, ParserConfiguration }
import org.tupol.utils._
import org.tupol.utils.config.Configurator
import scalaz.ValidationNel

import scala.util.Try

case class FileDataFrameLoader(configuration: FileDataFrameLoaderConfig) extends Logging {

  /** Create and configure a `DataFrameReader` based on the given `ParserConfiguration` */
  private def createReader(parserConfiguration: ParserConfiguration)(implicit spark: SparkSession): DataFrameReader = {

    val dataFormat = parserConfiguration.format.toString
    val basicReader = spark.read
      .format(dataFormat)
      .options(parserConfiguration.parserOptions)

    parserConfiguration.schema match {
      case Some(inputSchema) =>
        logDebug(s"Initializing the $dataFormat DataFrame loader using the specified schema.")
        val schema = parserConfiguration.columnNameOfCorruptRecord
          .map { columnNameOfCorruptRecord =>
            logDebug(s"The '$ColumnNameOfCorruptRecord' was specified; adding column '$columnNameOfCorruptRecord' to the input schema.")
            inputSchema.add(columnNameOfCorruptRecord, StringType)
          }
          .getOrElse(inputSchema)
        basicReader.schema(schema)
      case None =>
        logDebug(s"Initializing the $dataFormat DataFrame loader inferring the schema.")
        basicReader
    }
  }

  def loadData(implicit spark: SparkSession): Try[DataFrame] = {
    logInfo(s"Reading data as '${configuration.parserConfiguration.format}' from '${configuration.path}'.")
    Try(createReader(configuration.parserConfiguration).load(configuration.path))
      .logSuccess(d => logInfo(s"Successfully read the data as '${configuration.parserConfiguration.format}' from '${configuration.path}'"))
      .logFailure(t => logError(s"Failed to read the data as '${configuration.parserConfiguration.format}' from '${configuration.path}'", t))
  }

}

/**
 * Basic configuration for the `FileDataFrameLoader``
 * @param path
 * @param parserConfiguration
 */
case class FileDataFrameLoaderConfig(path: String, parserConfiguration: ParserConfiguration) {
  /** Get the format type of the input file. */
  def format: FormatType = parserConfiguration.format
  override def toString: String = s"path: '$path', parserConfiguration: $parserConfiguration"
}
object FileDataFrameLoaderConfig extends Configurator[FileDataFrameLoaderConfig] {
  override def validationNel(config: Config): ValidationNel[Throwable, FileDataFrameLoaderConfig] = {
    import org.tupol.utils.config._
    import scalaz.syntax.applicative._
    config.extract[String]("path") |@|
      ParserConfiguration.validationNel(config) apply
      FileDataFrameLoaderConfig.apply
  }
}
