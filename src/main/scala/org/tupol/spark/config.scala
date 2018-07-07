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

import com.typesafe.config.ConfigException.{ BadValue, Missing }
import com.typesafe.config.{ Config, ConfigObject }
import scalaz.syntax.validation._
import scalaz.{ NonEmptyList, ValidationNel }

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

package object config {

  /**
   * An extractor is a bit of code that knows how to extract a value of type T
   * from a typesafe {{Config}} instance.
   *
   * @tparam T the type that this implementation can extract for us.
   */
  trait Extractor[T] {

    /**
     * @param config the typesafe Config instance.
     * @param path the path at which the value resides.
     * @return the value that was retrieved for us.
     */
    def extract(config: Config, path: String): T
  }

  object Extractor {

    @implicitNotFound("No compatible Extractor in scope for type ${T}")
    def apply[T](implicit T: Extractor[T]): Extractor[T] = T

    implicit val stringExtractor = new Extractor[String] {
      def extract(config: Config, path: String): String = config.getString(path)
    }

    implicit val stringListExtractor = new Extractor[Seq[String]] {
      def extract(config: Config, path: String): Seq[String] = Seq(config.getStringList(path).asScala: _*)
    }

    implicit val doubleListExtractor = new Extractor[Seq[Double]] {
      def extract(config: Config, path: String): Seq[Double] = Seq(config.getDoubleList(path).asScala.map(_.doubleValue): _*)
    }

    implicit val configListExtractor = new Extractor[Seq[Config]] {
      def extract(config: Config, path: String): Seq[Config] = Seq(config.getConfigList(path).asScala: _*)
    }

    implicit val string2stringMapExtractor = new Extractor[Map[String, String]] {
      def extract(config: Config, path: String): Map[String, String] = string2AnyMapExtractor.extract(config, path).mapValues(_.toString)
    }

    implicit val string2intMapExtractor = new Extractor[Map[String, Int]] {
      def extract(config: Config, path: String): Map[String, Int] = string2AnyMapExtractor.extract(config, path).mapValues(_.toString.toInt)
    }

    implicit val string2longMapExtractor = new Extractor[Map[String, Long]] {
      def extract(config: Config, path: String): Map[String, Long] = string2AnyMapExtractor.extract(config, path).mapValues(_.toString.toLong)
    }

    implicit val string2doubleMapExtractor = new Extractor[Map[String, Double]] {
      def extract(config: Config, path: String): Map[String, Double] = string2AnyMapExtractor.extract(config, path).mapValues(_.toString.toDouble)
    }

    implicit val string2AnyMapExtractor: Extractor[Map[String, Any]] = new Extractor[Map[String, Any]] {
      def object2pairs(o: ConfigObject): Seq[(String, Any)] = for {
        entry <- o.entrySet().asScala.toSeq
        key = entry.getKey
        value = entry.getValue.unwrapped()
      } yield (key, value)

      def list2pairs(os: Seq[ConfigObject]): Seq[(String, Any)] = for {
        o <- os
        entry <- o.entrySet().asScala
        key = entry.getKey
        value = entry.getValue.unwrapped()
      } yield (key, value)

      def extract(config: Config, path: String): Map[String, Any] = Try(object2pairs(config.getObject(path)))
        .getOrElse(list2pairs(config.getObjectList(path).asScala)).toMap
    }

    implicit val characterExtractor = new Extractor[Character] {
      def extract(config: Config, path: String): Character = config.getString(path).charAt(0)
    }

    implicit val intExtractor = new Extractor[Int] {
      def extract(config: Config, path: String): Int = config.getInt(path)
    }

    implicit val longExtractor = new Extractor[Long] {
      def extract(config: Config, path: String): Long = config.getLong(path)
    }

    implicit val doubleExtractor = new Extractor[Double] {
      def extract(config: Config, path: String): Double = config.getDouble(path)
    }

    implicit val booleanExtractor = new Extractor[Boolean] {
      def extract(config: Config, path: String): Boolean = config.getBoolean(path)
    }

    implicit val rangeExtractor = new Extractor[Range] {
      /**
       * Construct a Range from a comma separated list of ints.
       *
       * Case 1: A sequence of ints
       * - first int: 'from'
       * - second int: 'to' (bigger than from)
       * - third int: 'step' a positive number
       *
       * Case 2: A single int
       *
       * @param value -> start,stop,step or just an int value
       * @param path -> optional path for this property
       * @return
       */
      def parseStringToRange(value: String, path: String = "unknown"): Range = Try(value.split(",").map(_.trim.toInt).toSeq) match {
        case Success(start +: stop +: _ +: Nil) if start > stop =>
          throw new BadValue(path, "The start should be smaller than the stop.")
        case Success(_ +: _ +: step +: Nil) if step < 0 =>
          throw new BadValue(path, "The step should be a positive number.")
        case Success(start +: stop +: step +: Nil) =>
          start to stop by step
        case Success(v +: Nil) =>
          v to v
        case _ =>
          throw new BadValue(path, "The input should contain either an integer or a comma separated list of 3 integers.")
      }
      def extract(config: Config, path: String): Range = parseStringToRange(config.getString(path), path)
    }

    /**
     * Allows us to extract an Option[T] for every T that we've defined an extractor for.
     *
     * @param extractor an implicit Extractor[T] that needs to be in scope
     * @tparam T the extracted value
     * @return A Some(T) if we can extract a valid property of the given type or a None otherwise.
     */
    implicit def optionExtractor[T](implicit extractor: Extractor[T]): Extractor[Option[T]] = new Extractor[Option[T]] {
      override def extract(config: Config, path: String): Option[T] = Try(extractor.extract(config, path)) match {
        case Success(value) => Some(value)
        case Failure(_) => None
      }
    }
  }

  /**
   * Adds the extract method to a typesafe Config instance, allowing us to request values from it like so:
   * {{config.extract[Double]("double")}} or {{config.extract[Option[Range]]("range")}}
   *
   * @param config the typesafe Config instance to operate on.
   */
  implicit class RichConfig(config: Config) {
    def extract[T: Extractor](path: String): ValidationNel[Throwable, T] = Try(Extractor[T].extract(config, path)) match {
      case Success(value) => value.success
      case Failure(exception: Throwable) => exception.failureNel
    }
    def validatePath(path: String): ValidationNel[Throwable, String] =
      if (config.hasPath(path)) path.successNel
      else (new Missing(path): Throwable).failureNel
  }

  /**
   * Add a convenience method to convert an Exception to a NonEmptyList[Throwable].
   * @param throwable
   */
  implicit class ThrowableOps(val throwable: Throwable) extends AnyVal {
    def toNel: NonEmptyList[Throwable] = NonEmptyList(throwable)
  }

  /**
   * Implicit conversion from ValidationNel[Exception,T] to Try[T]. Using this construct allows
   * us to keep the Validation logic contained in configuration code, keeping the rest of our code scalaz agnostic.
   */
  import scala.language.implicitConversions
  implicit def validationNelToTry[E <: Throwable, T](validation: ValidationNel[E, T]): Try[T] = validation match {
    case scalaz.Failure(exceptions) => Failure(ConfigurationException(exceptions.list.toList))
    case scalaz.Success(value) => Success(value)
  }

  /**
   * Encapsulates all configuration exceptions that occurred while trying to map
   * the data from a typesafe Config object onto a case class.
   */
  case class ConfigurationException(errors: Seq[Throwable]) extends Exception {
    override def getMessage: String =
      ("Invalid configuration. Please check the issue(s) listed bellow:" +:
        errors.map(error => s"- ${error.getMessage}")).
        mkString("\n")
    override def toString = s"ConfigurationException[${getMessage()}]"
  }

  /**
   * Implementors of this trait how to construct a class of type T from a typesafe Config instance.
   * @tparam T, the type of class this Configurator knows how to validate/construct.
   */
  trait Configurator[T] {
    def validationNel(config: Config): ValidationNel[Throwable, T]
    def apply(config: Config): Try[T] = validationNel(config)
  }
}
