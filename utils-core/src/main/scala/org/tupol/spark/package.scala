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
package org.tupol

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

package object spark {

  def simpleClassName(any: Any): String = any.getClass.getSimpleName.replaceAll("\\$", "")

  object encoders {

    /**
     * This code is taken from `org.apache.spark.sql.catalyst.encoders` to work around a Databricks runtime issue.
     *
     * Returns an internal encoder object that can be used to serialize / deserialize JVM objects
     * into Spark SQL rows.  The implicit encoder should always be unresolved (i.e. have no attribute
     * references from a specific schema.)  This requirement allows us to preserve whether a given
     * object type is being bound by name or by ordinal when doing resolution.
     */
    def encoderFor[A: Encoder]: ExpressionEncoder[A] =
      implicitly[Encoder[A]] match {
        case e: ExpressionEncoder[A] =>
          e.assertUnresolved()
          e
        case _ => sys.error(s"Only expression encoders are supported today")
      }

    def tuple2[A: Encoder, B: Encoder]: Encoder[(A, B)] =
      org.apache.spark.sql.Encoders.tuple(encoderFor[A], encoderFor[B])
  }
}
