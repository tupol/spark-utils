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

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SparkSession }
import org.tupol.spark.Logging
import org.tupol.spark.io.sources.{ ColumnNameOfCorruptRecord, GenericSourceConfiguration }
import org.tupol.utils.implicits._

import scala.util.Try

case class GenericDataSource(configuration: GenericSourceConfiguration)
  extends DataSource[GenericSourceConfiguration, DataFrameReader] with Logging {

  /** Create and configure a `DataFrameReader` based on the given `GenericDataSourceConfig` */
  def reader(implicit spark: SparkSession): DataFrameReader = {
    val dataFormat = configuration.format.toString
    val basicReader = spark.read
      .format(dataFormat)
      .options(configuration.options)

    configuration.schema match {
      case Some(inputSchema) =>
        logDebug(s"Initializing the '$dataFormat' DataFrame loader using the specified schema.")
        val schema = configuration.columnNameOfCorruptRecord
          .map { columnNameOfCorruptRecord =>
            logDebug(s"The '$ColumnNameOfCorruptRecord' was specified; " +
              s"adding column '$columnNameOfCorruptRecord' to the input schema.")
            inputSchema.add(columnNameOfCorruptRecord, StringType)
          }
          .getOrElse(inputSchema)
        basicReader.schema(schema)
      case None =>
        logDebug(s"Initializing the '$dataFormat' DataFrame loader inferring the schema.")
        basicReader
    }
  }

  /** Try to read the data according to the given configuration and return the read data or a failure */
  override def read(implicit spark: SparkSession): Try[DataFrame] = {
    logInfo(s"Reading data as '${configuration.format}' from '${configuration}'.")
    Try(reader.load())
      .mapFailure(DataSourceException(s"Failed to read the data as '${configuration.format}' from '${configuration}'", _))
      .logFailure(logError)
      .logSuccess(_ => logInfo(s"Successfully read the data " +
        s"as '${configuration.format}' from '${configuration}"))
  }

}
