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

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Dataset, Row, SparkSession}
import org.tupol.spark.io.streaming.structured.StreamingSourceFactory
import org.tupol.spark.sql


package object implicits {

  /** SparkSession decorator. */
  implicit class SparkSessionOps(spark: SparkSession) {
    /** See [[org.tupol.spark.io.DataSource]] */
    def source[SC <: DataSourceConfiguration](configuration: SC)(implicit sourceFactory: DataSourceFactory): DataSource[SC, DataFrameReader] =
      sourceFactory(configuration)

    /** See [[org.tupol.spark.io.DataSource]] */
    def streamingSource[SC <: DataSourceConfiguration](configuration: SC)(implicit sourceFactory: StreamingSourceFactory): DataSource[SC, DataStreamReader] =
      sourceFactory(configuration)
  }

  /** DataFrame decorator. */
  implicit class DataFrameOps(val dataFrame: DataFrame) {

    /** See [[org.tupol.spark.sql.flattenFields()]] */
    def flattenFields: DataFrame = sql.flattenFields(dataFrame)

    /** Not all column names are compliant to the Avro format. This function renames to columns to be Avro compliant */
    def makeAvroCompliant(implicit spark: SparkSession): DataFrame =
      sql.makeDataFrameAvroCompliant(dataFrame)

    /** See [[org.tupol.spark.io.DataSink]] */
    def sink[SC <: DataSinkConfiguration](configuration: SC)(implicit sinkFactory: DataAwareSinkFactory): DataAwareSink[SC, DataFrameWriter[Row], DataFrame] =
      sinkFactory.apply[SC, DataFrameWriter[Row], DataFrame](configuration, dataFrame)

    /** See [[org.tupol.spark.io.DataSink]] */
    def streamingSink[SC <: DataSinkConfiguration](configuration: SC)(implicit sinkFactory: DataAwareSinkFactory): DataAwareSink[SC, DataStreamWriter[Row], StreamingQuery] =
      sinkFactory.apply[SC, DataStreamWriter[Row], StreamingQuery](configuration, dataFrame)

  }

  /** Dataset decorator. */
  implicit class DatasetOps[T](val dataset: Dataset[T]) {

    /** See [[org.tupol.spark.io.DataSink]] */
    def sink[SC <: DataSinkConfiguration](configuration: SC)(implicit sinkFactory: DataAwareSinkFactory): DataAwareSink[SC, DataFrameWriter[Row], DataFrame] =
      sinkFactory.apply[SC, DataFrameWriter[Row], DataFrame](configuration, dataset.toDF())

    /** See [[org.tupol.spark.io.DataSink]] */
    def streamingSink[SC <: DataSinkConfiguration](configuration: SC)(implicit sinkFactory: DataAwareSinkFactory): DataAwareSink[SC, DataStreamWriter[Row], StreamingQuery] =
      sinkFactory.apply[SC, DataStreamWriter[Row], StreamingQuery](configuration, dataset.toDF())

  }

}
