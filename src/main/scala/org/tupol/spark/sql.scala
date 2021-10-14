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

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame, Row, SparkSession }

import scala.io.Source
import scala.reflect.runtime.universe._
import scala.util.Try

package object sql {

  /**
   * Extract the schema for a given type.
   * @tparam T the type to extract the schema for
   * @return
   */
  def schemaFor[T: TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  /**
   * Load a schema ([[StructType]]) from a json string.
   * @param json
   * @return
   */
  def loadSchemaFromString(json: String): Try[StructType] =
    Try(DataType.fromJson(json).asInstanceOf[StructType])

  /**
   * Load a schema ([[StructType]]) from a given file. The schema must be in json format.
   * @param resourcePath
   * @return
   */
  def loadSchemaFromFile(resourcePath: String): Try[StructType] =
    for {
      json <- Try(Source.fromFile(resourcePath).getLines.mkString(" "))
      schema <- loadSchemaFromString(json)
    } yield schema

  /**
   * "Flatten" a DataFrame.
   *
   * If the DataFrame contains nested types, they are all unpacked to individual columns, having the column name as a
   * string representing the original hierarchy, separated by `_` characters.
   *
   * For example, a column containing a structure like `someCol: { fieldA, fieldB }` will be transformed into two columns
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
   * Make strings, as column names Avro compliant ([[https://avro.apache.org/docs/1.8.1/spec.html#names]])
   * All the illegal characters are replaced with the `replaceWith` string which is '_' by default.
   * Prefix and suffix can be defined as well.
   * The replacement character, the prefix and suffix must also be Avro compliant.
   *
   * @param string the string to be transformed to an Avro compliant string
   * @param replaceWith the string that will replace not compliant Avro characters
   * @param prefix the string that will prefix the non-compliant Avro strings
   * @param suffix the string that will suffix the non-compliant Avro strings
   * @return same string if the string was Avro compliant of a compliant Avro name string
   */
  private[sql] def makeNameAvroCompliant(string: String, replaceWith: String, prefix: String, suffix: String) = {

    def acceptableFirstChar(char: Char): Boolean = (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '_'
    def acceptableTailChar(char: Char): Boolean = char.isDigit || acceptableFirstChar(char)
    def illegalContentChars(string: String) = string.filterNot(acceptableTailChar).toSet
    def requireFirstChar(string: String, printName: String) =
      require(
        if (string.size > 0) acceptableFirstChar(string.head) else true,
        s"The $printName starts with an illegal Avro character: '${string.head}'.")
    def requireContentChars(string: String, printName: String) =
      require(
        if (string.size > 0) illegalContentChars(string).size == 0 else true,
        s"The $printName contains illegal Avro character(s): '${illegalContentChars(string).mkString("'", ", ", "'")}'.")

    require(string.nonEmpty, "The input string can not be empty.")

    if (prefix.nonEmpty) {
      requireFirstChar(prefix, "prefix")
      requireContentChars(prefix, "prefix")
    } else {
      requireFirstChar(replaceWith, "replacement string")
    }
    requireContentChars(replaceWith, "replacement string")
    requireContentChars(suffix, "suffix")

    val first = if (prefix.isEmpty && !acceptableFirstChar(string.head)) replaceWith else string.head.toString
    val body = string.tail.flatMap { c => if (acceptableTailChar(c)) Seq(c) else replaceWith }

    prefix + first + body + suffix

  }

  /**
   * Transform the DataFrame column names to be Avro compliant ([[https://avro.apache.org/docs/1.8.1/spec.html#names]])
   * All the illegal characters are replaced with the `replaceWith` string which is '_' by default.
   * Prefix and suffix can be defined as well.
   * The replacement character, the prefix and suffix must also be Avro compliant.
   *
   * @param dataFrame the DataFrame to be transformed to a DataFrame with an Avro compliant schema
   * @param replaceWith the string that will replace not compliant Avro characters
   * @param prefix the string that will prefix the non-compliant Avro strings
   * @param suffix the string that will suffix the non-compliant Avro strings
   * @param spark SparkSession
   * @return
   */
  def makeDataFrameAvroCompliant(dataFrame: DataFrame, replaceWith: String = "_", prefix: String = "", suffix: String = "")(implicit spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.types.MetadataBuilder
    import org.tupol.spark.implicits._
    val newSchema = dataFrame.schema.mapFields { field =>
      val newFieldName = makeNameAvroCompliant(field.name, replaceWith, prefix, suffix)
      val newMetadata = new MetadataBuilder().withMetadata(field.metadata).putString("originalColumnName", field.name).build()
      field.copy(name = newFieldName, metadata = newMetadata)
    }
    spark.createDataFrame(dataFrame.rdd, newSchema)
  }

}
