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

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util

import org.json4s.JsonAST.JString
import org.json4s.{ CustomSerializer, Serializer }

import scala.util.{ Failure, Success, Try }

/**
 * A few common functions that might be useful.
 */
package object utils {

  /**
   * Run a block and return the block result and the runtime in millis
   * @param block
   * @return
   */
  def timeCode[T](block: => T): (T, Long) = {
    val start = System.currentTimeMillis
    val result = block
    val end = System.currentTimeMillis
    (result, end - start)
  }

  /**
   * Simple decorator for the scala Try, which adds the following simple functions:
   * - composable error logging
   * - composable success logging
   * @param attempt
   * @tparam T
   */
  implicit class TryOps[T](val attempt: Try[T]) extends AnyVal {

    /**
     * Log the error using the logging function and return the failure
     * @param logging
     * @return
     */
    def logFailure(logging: (Throwable) => Unit) =
      attempt.recoverWith {
        case t: Throwable =>
          logging(t)
          Failure(t)
      }

    /**
     * Log the success using the logging function and return the attempt
     * @param logging
     * @return
     */
    def logSuccess(logging: (T) => Unit): Try[T] =
      attempt.map { t =>
        logging(t)
        t
      }

    /**
     * Log the progress and return back the try.
     * @param successLogging
     * @param failureLogging
     * @return
     */
    def log(successLogging: (T) => Unit, failureLogging: (Throwable) => Unit): Try[T] = {
      attempt match {
        case Success(s) => successLogging(s)
        case Failure(t) => failureLogging(t)
      }
      attempt
    }
  }

  /**
   * Flatten a sequence of Trys to a try of sequence, which is a failure if any if the Trys is a failure.
   * @param seqOfTry
   * @tparam T
   * @return
   */
  def allOkOrFail[T](seqOfTry: Seq[Try[T]]): Try[Seq[T]] =
    seqOfTry.foldLeft(Try(Seq[T]())) { (acc, tryField) => tryField.flatMap(tf => acc.map(tf +: _)) }

  /**
   * Simple decorator for the scala Try, which adds the following simple functions:
   * @param seqOfTry
   * @tparam T
   */
  implicit class SeqTryOps[T](val seqOfTry: Seq[Try[T]]) extends AnyVal {

    /**
     * Flatten a sequence of Trys to a try of sequence, which is a failure if any of the Trys is a failure.
     * @return
     */
    def allOkOrFail: Try[Seq[T]] = utils.allOkOrFail(seqOfTry)

  }

  /**
   * This is a small and probably wrong conversion to JSON format.
   *
   * Besides the basic conversion, this also serializes the LocalDateFormat
   *
   * @param input input to be converted to JSON format
   * @return
   */
  def toJson(input: AnyRef) = {
    //TODO Find a nicer more comprehensive solution
    import org.json4s.NoTypeHints
    import org.json4s.jackson.Serialization

    implicit val formats = Serialization.formats(NoTypeHints) ++ TimeSerializers
    Serialization.write(input)
  }

  /**
   * Serializers for Time types that use commonly use in MLX suite
   * @return
   */
  lazy val TimeSerializers: Seq[Serializer[_]] = {
    /**
     * Serializer / deserializer for LocalDateFormat
     */
    case object LDTSerializer extends CustomSerializer[LocalDateTime](format => (
      { case JString(s) => LocalDateTime.parse(s) },
      { case ldt: LocalDateTime => JString(ldt.toString) }
    ))
    /**
     * Serializer / deserializer for Timestamp
     */
    case object SqlTimestampSerializer extends CustomSerializer[Timestamp](format => (
      { case JString(ts) => Timestamp.valueOf(LocalDateTime.parse(ts)) },
      { case ts: Timestamp => JString(ts.toLocalDateTime.toString) }
    ))

    Seq(LDTSerializer, SqlTimestampSerializer)
  }

  import org.apache.spark.sql.catalyst.ScalaReflection
  import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
  import org.apache.spark.sql.types.{ StructField, StructType }
  import org.apache.spark.sql.{ Column, DataFrame, Row }
  import org.json4s.NoTypeHints
  import org.json4s.jackson.Serialization

  import scala.reflect.runtime.universe._

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
   * Product decorator
   * @param product
   */
  implicit class ProductOps(product: Product) {
    import org.json4s.Extraction

    /**
     * Convert the product into a map keyed by field name
     * @return
     */
    def toMap: Map[String, Any] = {
      val formats = Serialization.formats(NoTypeHints) ++ TimeSerializers
      Extraction.decompose(product)(formats).values.asInstanceOf[Map[String, Any]]
    }
  }

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
  def flattenDataFrame(dataFrame: DataFrame) = {

    def constructFlatteningInstructions(field: StructField, ancestors: Seq[String] = Nil): Seq[(String, String)] = field.dataType match {
      case StructType(children) =>
        children.flatMap(child => constructFlatteningInstructions(child, ancestors :+ field.name))
      case _ =>
        val fullPath = ancestors :+ field.name
        Seq((fullPath.mkString("."), fullPath.mkString("_")))
    }

    val flatteningInstructions = dataFrame.schema.fields.toSeq
      .flatMap(field => constructFlatteningInstructions(field))
      .map { case (originalPath, aliasedName) => new Column(originalPath).as(aliasedName) }

    dataFrame.select(flatteningInstructions: _*)
  }

  implicit class DataFrameOps(val dataFrame: DataFrame) {
    /**
     * See [[io.tupol.spark.utils.flattenDataFrame()]]
     *
     * @return
     */
    def flatten: DataFrame = flattenDataFrame(dataFrame)
  }

}
