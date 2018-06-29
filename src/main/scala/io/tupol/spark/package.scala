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
package io.tupol.spark

import java.util

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame, Row }

import scala.reflect.runtime.universe._

package object sql {

  /**
   * Extract the schema for a given type.
   * @tparam T the type to extract the schema for
   * @return
   */
  def schemaFor[T: TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  /**
   * "Flatten" a DataFrame.
   *
   * If the DataFrame contains nested types, they are all unpacked to individual columns, having the column name as a
   * string representing the original hierarchy, separated by `_` characters.
   *
   * For example, a column containing a structure like `someCol: { fieldA, fieldB}` will be transformed into two columns
   * like `someCol_fieldA` and `someCol_fieldB`.
   *
   * @param dataFrame
   * @return
   */
  def flattenFields(dataFrame: DataFrame): DataFrame = {

    // Create a list of aliases for the given field or structure field
    def createAliases(field: StructField, ancestors: Seq[String] = Nil): Seq[(String, String)] =
      field.dataType match {
        case StructType(children) =>
          children.flatMap(child => createAliases(child, ancestors :+ field.name))
        case _ =>
          val fullPath = ancestors :+ field.name
          Seq((fullPath.mkString("."), fullPath.mkString("_")))
      }

    val selectColumns = dataFrame.schema.fields.toSeq
      .flatMap(field => createAliases(field))
      .map { case (originalPath, aliasedName) => new Column(originalPath).as(aliasedName) }

    dataFrame.select(selectColumns: _*)
  }

  /**
   * Transform the fields inside a data type recursively
   * @param dataType sequence of fields
   * @param mapFun fields mapping function
   * @return
   */
  def mapFields(dataType: DataType, mapFun: StructField => StructField): DataType =
    dataType match {
      case StructType(children) =>
        val mappedFields = children.map(mapFun).map {
          field =>
            val newDataType = mapFields(field.dataType, mapFun)
            field.copy(dataType = newDataType)
        }
        StructType(mappedFields)
      case ArrayType(dt, containsNull) =>
        ArrayType(mapFields(dt, mapFun), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(mapFields(keyType, mapFun), mapFields(valueType, mapFun), valueContainsNull)
      case dt => dt
    }

  /**
   * Check that the fields satisfy the predicate and computes the final result using a reducing function
   * @param dataType
   * @param predicate
   * @param reducer
   * @param result
   * @return
   */
  def checkFields(dataType: DataType, predicate: StructField => Boolean, reducer: (Boolean, Boolean) => Boolean, result: Boolean): Boolean = {

    dataType match {
      case StructType(children) =>
        val checkedFields = children.map(predicate).reduce(reducer)
        children.map(_.dataType).map(checkFields(_, predicate, reducer, checkedFields)).reduce(reducer)
      case ArrayType(dataType, _) => checkFields(dataType, predicate, reducer, result)
      case MapType(keyType, valueType, _) =>
        checkFields(keyType, predicate, reducer, checkFields(valueType, predicate, reducer, result))
      case _ => result
    }
  }

  /**
   * Check if all the fields of the given schema satisfy the predicate
   * @param schema
   * @param predicate
   * @return
   */
  def checkAllFields(schema: DataType, predicate: StructField => Boolean): Boolean =
    checkFields(schema, predicate, _ && _, true)

  /**
   * Check if any of the fields of the given schema satisfy the given predicate
   * @param schema
   * @param predicate
   * @return
   */
  def checkAnyFields(schema: DataType, predicate: StructField => Boolean): Boolean =
    checkFields(schema, predicate, _ || _, false)

  implicit class SchemaOps(val schema: StructType) {

    /**
     * See [[io.tupol.spark.sql.mapFields()]]
     *
     * @return
     */
    def mapFields(mapFun: StructField => StructField): StructType =
      sql.mapFields(schema, mapFun).asInstanceOf[StructType]

    /**
     * See [[io.tupol.spark.sql.checkAllFields()]]
     *
     * @return
     */
    def checkAllFields(predicate: StructField => Boolean): Boolean = sql.checkAllFields(schema, predicate)

    /**
     * See [[io.tupol.spark.sql.checkAnyFields()]]
     *
     * @return
     */
    def checkAnyFields(predicate: StructField => Boolean): Boolean = sql.checkAnyFields(schema, predicate)
  }

  implicit class DataFrameOps(val dataFrame: DataFrame) {
    /**
     * See [[io.tupol.spark.sql.flattenFields()]]
     *
     * @return
     */
    def flattenFields: DataFrame = sql.flattenFields(dataFrame)
  }
  /**
   * Simple conversion between a Spark SQL Row and a Map. Inner rows are also transformed also into maps.
   * @param row
   * @return
   */
  def row2map(row: Row): Map[String, Any] =
    row.schema.fields.map { key =>
      val value = row.getAs[Any](key.name) match {
        case r: Row => row2map(r)
        case v => v
      }
      (key.name, value)
    }.toMap

  /**
   * Row decorator.
   *
   * @param row
   */
  implicit class RowOps(val row: Row) {
    // Primitive conversion from Map to a Row using a schema
    // TODO: this does not take care of inner maps
    def toMap: Map[String, Any] = row2map(row)
  }

  /**
   * Map decorator with a Spark flavour
   * @param map
   * @tparam K
   * @tparam V
   */
  implicit class MapOps[K, V](val map: Map[K, V]) {
    // Primitive conversion from Map to a Row using a schema
    // TODO: Deal with inner maps
    def toRow(schema: StructType): Row = {
      val values = schema.fieldNames.map(fieldName => map.map { case (k, v) => (k.toString(), v) }.get(fieldName).getOrElse(null))
      new GenericRowWithSchema(values, schema)
    }
    def toHashMap = {
      val props = new util.HashMap[K, V]()
      map.foreach { case (k, v) => props.put(k, v) }
      props
    }
  }

}
