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
package org.tupol.spark

import com.typesafe.config.{ Config, ConfigRenderOptions }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.tupol.spark.io._
import org.tupol.spark.sql.{ loadSchemaFromString, row2map }
import org.tupol.spark.utils._
import org.tupol.utils.config.Extractor

package object implicits {

  /** Row decorator. */
  implicit class RowOps(val row: Row) {
    // Primitive conversion from Map to a Row using a schema
    // TODO: this does not take care of inner maps
    def toMap: Map[String, Any] = row2map(row)
  }

  /** Map decorator with a Spark flavour */
  implicit class MapOps[K, V](val map: Map[K, V]) {
    // Primitive conversion from Map to a Row using a schema
    // TODO: Deal with inner maps
    def toRow(schema: StructType): Row = {
      val values = schema.fieldNames.map(fieldName => map.map { case (k, v) => (k.toString(), v) }.get(fieldName).getOrElse(null))
      new GenericRowWithSchema(values, schema)
    }
  }

  /** Product decorator */
  implicit class ProductOps(product: Product) {
    import org.json4s.jackson.Serialization
    import org.json4s.{ Extraction, NoTypeHints }

    /** Convert the product into a map keyed by field name */
    def toMap: Map[String, Any] = {
      val formats = Serialization.formats(NoTypeHints) ++ TimeSerializers
      Extraction.decompose(product)(formats).values.asInstanceOf[Map[String, Any]]
    }
  }

  /**
   * Configuration extractor for Schemas.
   *
   * It can be used as
   * `config.extract[Option[StructType]]("configuration_path_to_schema")` or as
   * `config.extract[StructType]("configuration_path_to_schema")`
   */
  implicit val StructTypeExtractor = new Extractor[StructType] {
    def extract(config: Config, path: String): StructType = {
      val schema = config.getObject(path).render(ConfigRenderOptions.concise())
      loadSchemaFromString(schema)
    }
  }

  /** SparkSession decorator. */
  implicit class SparkSessionOps(spark: SparkSession) {
    /** See [[org.tupol.spark.io.DataSource]] */
    def source[SC <: DataSourceConfiguration](configuration: SC)(implicit sourceFactory: DataSourceFactory): DataSource[SC] =
      sourceFactory(configuration)
  }

  /** StructType decorator. */
  implicit class SchemaOps(val schema: StructType) {

    /** See [[org.tupol.spark.sql.mapFields()]] */
    def mapFields(mapFun: StructField => StructField): StructType =
      sql.mapFields(schema, mapFun).asInstanceOf[StructType]

    /** See [[org.tupol.spark.sql.checkAllFields()]] */
    def checkAllFields(predicate: StructField => Boolean): Boolean = sql.checkAllFields(schema, predicate)

    /** See [[org.tupol.spark.sql.checkAnyFields()]] */
    def checkAnyFields(predicate: StructField => Boolean): Boolean = sql.checkAnyFields(schema, predicate)
  }

  /** DataFrame decorator. */
  implicit class DataFrameOps(val dataFrame: DataFrame) {

    /** See [[org.tupol.spark.sql.flattenFields()]] */
    def flattenFields: DataFrame = sql.flattenFields(dataFrame)

    /** See [[org.tupol.spark.io.DataSink]] */
    def sink[SC <: DataSinkConfiguration](configuration: SC)(implicit sinkFactory: DataAwareSinkFactory): DataAwareSink[SC, DataFrame] =
      sinkFactory.apply[SC, DataFrame](configuration, dataFrame)

    /** Not all column names are compliant to the Avro format. This function renames to columns to be Avro compliant */
    def makeAvroCompliant(implicit spark: SparkSession): DataFrame =
      sql.makeDataFrameAvroCompliant(dataFrame)
  }

}
