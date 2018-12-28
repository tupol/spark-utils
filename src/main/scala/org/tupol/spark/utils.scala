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

import java.net.{ URI, URL }
import java.sql.Timestamp
import java.time.LocalDateTime

import org.json4s.JsonAST.JString
import org.json4s.{ CustomSerializer, Serializer }

import org.tupol.utils._

import scala.io.Source
import scala.util.{ Failure, Try }

/** A few common functions that might be useful. */
package object utils extends Logging {

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

  /** Serializers for Time types that use commonly */
  lazy val TimeSerializers: Seq[Serializer[_]] = {
    /** Serializer / deserializer for LocalDateFormat */
    case object LDTSerializer extends CustomSerializer[LocalDateTime](format => (
      { case JString(s) => LocalDateTime.parse(s) },
      { case ldt: LocalDateTime => JString(ldt.toString) }))
    /** Serializer / deserializer for Timestamp */
    case object SqlTimestampSerializer extends CustomSerializer[Timestamp](format => (
      { case JString(ts) => Timestamp.valueOf(LocalDateTime.parse(ts)) },
      { case ts: Timestamp => JString(ts.toLocalDateTime.toString) }))
    Seq(LDTSerializer, SqlTimestampSerializer)
  }

  /**
   * Try loading a text resource from a given path, whether it is local, from a given URL or URI or from the classpath.
   * Also, the order of attempting to solce the resource file corresponds to: they local then try URI then try URL then try classpath.
   *
   * @param path
   * @return the unix new line separated text
   */
  def fuzzyLoadTextResourceFile(path: String): Try[String] = {

    val bufferedSource = if (path.trim.isEmpty)
      Failure(new IllegalArgumentException(s"Cannot load a text resource from an empty path."))
    else {
      Try {
        logDebug(s"Try loading text resource from local file '$path'.")
        Source.fromFile(path)
      }
        .logSuccess(_ => logDebug(s"Successfully loaded resource from local path '$path'."))
        .orElse {
          logDebug(s"Try loading text resource from URI '$path'.")
          Try(Source.fromURI(new URI(path)))
            .logSuccess(_ => logDebug(s"Successfully loaded resource from URI '$path'."))
        }
        .orElse {
          logDebug(s"Try loading text resource from URL '$path'.")
          Try(Source.fromURL(new URL(path)))
            .logSuccess(_ => logDebug(s"Successfully loaded resource from URL '$path'."))
        }
        .orElse {
          logDebug(s"Try loading text resource from classpath '$path'.")
          val bufferedSource =
            Option(Thread.currentThread.getContextClassLoader.getResourceAsStream(path))
              .orElse(Option(Thread.currentThread.getClass.getResourceAsStream(path))) match {
                case Some(inputStream) => Try(Source.fromInputStream(inputStream))
                case None => Failure(new IllegalArgumentException(s"Unable to find '$path' in the classpath."))
              }
          bufferedSource
            .logSuccess(_ => logDebug(s"Successfully loaded text resource from classpath '$path'."))
        }
    }
    bufferedSource.map(_.getLines.mkString("\n"))
      .logFailure(logError(s"Failed to load text resource from '$path'.", _))
  }

}
