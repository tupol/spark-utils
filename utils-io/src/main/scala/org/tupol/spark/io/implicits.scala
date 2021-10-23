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

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tupol.configz.Extractor
import org.tupol.spark.sql
import org.tupol.spark.sql.loadSchemaFromString

import scala.util.Try


package object implicits {

  /**
   * Configuration extractor for Schemas.
   *
   * It can be used as
   * `config.extract[Option[StructType]]("configuration_path_to_schema")` or as
   * `config.extract[StructType]("configuration_path_to_schema")`
   */
  implicit val StructTypeExtractor = new Extractor[StructType] {
    def extract(config: Config, path: String): Try[StructType] =
      for {
        schemaJson <- Try(config.getObject(path).render(ConfigRenderOptions.concise()))
        schema <- loadSchemaFromString(schemaJson)
      } yield schema
  }

  /** SparkSession decorator. */
  implicit class SparkSessionOps(spark: SparkSession) {
    /** See [[org.tupol.spark.io.DataSource]] */
    def source[SC <: DataSourceConfiguration](configuration: SC)(implicit sourceFactory: DataSourceFactory): DataSource[SC] =
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
    def sink[SC <: DataSinkConfiguration](configuration: SC)(implicit sinkFactory: DataAwareSinkFactory): DataAwareSink[SC, DataFrame] =
      sinkFactory.apply[SC, DataFrame](configuration, dataFrame)

    /** See [[org.tupol.spark.io.DataSink]] */
    def streamingSink[SC <: DataSinkConfiguration](configuration: SC)(implicit sinkFactory: DataAwareSinkFactory): DataAwareSink[SC, StreamingQuery] =
      sinkFactory.apply[SC, StreamingQuery](configuration, dataFrame)

  }

}
