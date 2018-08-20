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

import java.sql.Timestamp
import java.time.LocalDateTime

import org.json4s.JsonAST.JString
import org.json4s.{ CustomSerializer, Serializer }

/**
 * A few common functions that might be useful.
 */
package object utils {

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

  /**
   * Product decorator
   * @param product
   */
  implicit class ProductOps(product: Product) {
    import org.json4s.{ Extraction, NoTypeHints }
    import org.json4s.jackson.Serialization

    /**
     * Convert the product into a map keyed by field name
     * @return
     */
    def toMap: Map[String, Any] = {
      val formats = Serialization.formats(NoTypeHints) ++ TimeSerializers
      Extraction.decompose(product)(formats).values.asInstanceOf[Map[String, Any]]
    }
  }

}
