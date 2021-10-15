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

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.tupol.spark.sql.{loadSchemaFromString, row2map}
import org.tupol.spark.utils._

import java.util.UUID
import scala.util.Try

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

    /** Not all column names are compliant to the Avro format. This function renames to columns to be Avro compliant */
    def makeAvroCompliant(implicit spark: SparkSession): DataFrame =
      sql.makeDataFrameAvroCompliant(dataFrame)
  }

  implicit class DatasetOps[T: Encoder](val dataset: Dataset[T]) extends Serializable {

    import org.apache.spark.sql.{ Column, Dataset, Encoder }

    /**
     * Add a column to a dataset resulting in a dataset of a tuple of the type contained in the input dataset and the new column
     *
     * Sample usage:
     * {{{
     *   import nl.rabobank.datalake.common.spark.implicits._
     *   import org.apache.spark.sql.functions.lit
     *   import org.apache.spark.sql.Dataset
     *
     *   val dataset: Dataset[MyClass] = ...
     *   val datasetWithCol: Dataset[(MyClass, String)] = dataset.withColumnDataset[String](lit("some text"))
     * }}}
     *
     * @param column the column to be added
     * @tparam U The type of the added column
     * @return a Dataset containing a tuple of the input data and the given column
     */
    def withColumnDataset[U: Encoder](column: Column): Dataset[(T, U)] = {
      implicit val tuple2Encoder: Encoder[(T, U)] = encoders.tuple2[T, U]
      val tempColName = s"temp_col_${UUID.randomUUID()}"

      val tuple1 =
        if (dataset.encoder.clsTag.runtimeClass.isPrimitive) dataset.columns.map(col).head
        else struct(dataset.columns.map(col): _*)

      dataset
        .withColumn(tempColName, column)
        .select(tuple1 as "_1", col(tempColName) as "_2")
        .as[(T, U)]
    }
  }

  implicit class KeyValueDatasetOps[K: Encoder, V: Encoder](val dataset: Dataset[(K, V)]) extends Serializable {

    /**
     * Map values of a Dataset containing a key-value pair in a Tuple2
     *
     * Sample usage:
     * {{{
     *   import nl.rabobank.datalake.common.spark.implicits._
     *   import org.apache.spark.sql.Dataset
     *
     *   val dataset: Dataset[(String, Int)] = ...
     *   val result: Dataset[(String, Int)]  = dataset.mapValues(_ * 10)
     * }}}
     */
    def mapValues[U: Encoder](f: V => U): Dataset[(K, U)] = {
      implicit val tuple2Encoder: Encoder[(K, U)] = encoders.tuple2[K, U]
      dataset.map { case (k, v) => (k, f(v)) }
    }

    /**
     * FlatMap values of a Dataset containing a key-value pair in a Tuple2
     *
     * Sample usage:
     * {{{
     *   import nl.rabobank.datalake.common.spark.implicits._
     *   import org.apache.spark.sql.Dataset
     *
     *   val dataset: Dataset[(String, Int)] = ...
     *   val result: Dataset[(String, Int)]  = dataset.flatMapValues(Seq(1, 2, 3))
     * }}}
     */
    def flatMapValues[U: Encoder](f: V => TraversableOnce[U]): Dataset[(K, U)] = {
      implicit val tuple2Encoder: Encoder[(K, U)] = encoders.tuple2[K, U]
      dataset.flatMap { case (k, v) => f(v).map((k, _)) }
    }
  }

}
