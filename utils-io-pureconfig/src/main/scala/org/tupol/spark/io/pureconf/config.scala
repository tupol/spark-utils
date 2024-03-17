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
package org.tupol.spark.io.pureconf

import com.typesafe.config.Config
import org.tupol.spark.io.pureconf.errors.ConfigError
import pureconfig.{ ConfigReader, ConfigSource }
import org.tupol.utils.implicits._

import scala.util.Try

object config {

  /**
   * This function extracts a configuration instance from a given Typesafe/Lightbend configuration object.
   * It is using the `pureconfig.io` library behind the scenes.
   * <p/>The dependency to `pureconfig` needs to be provided in
   * the project using this.
   * <p/>To make life easy, have the following import wherever using this:
   *
   * {{{ import pureconfig.generic.auto._ }}}
   *
   * @param config configuration object to extract the target configuration object from
   * @tparam T type of the extracted object
   * @return an attempt to extract `T`
   */
  implicit class ConfigOps(val config: Config) {
    import org.tupol.spark.io.pureconf.{ config => packageConf }
    def extract[T](implicit reader: ConfigReader[T]): Try[T] = packageConf.extract[T](config)
    def extract[T](path: String)(implicit reader: ConfigReader[T]): Try[T] =
      packageConf.extract[T](config, path)(reader)
  }

  /**
   * This function extracts a configuration instance from a given Typesafe/Lightbend configuration object.
   * It is using the `pureconfig.io` library behind the scenes.
   * <p/>The dependency to `pureconfig` needs to be provided in
   * the project using this.
   * <p/>To make life easy, have the following import wherever using this:
   *
   * {{{ import pureconfig.generic.auto._ }}}
   *
   * @param config configuration object to extract the target configuration object from
   * @tparam T type of the extracted object
   * @return an attempt to extract `T`
   */
  def extract[T](config: Config)(implicit reader: ConfigReader[T]): Try[T] =
    ConfigSource
      .fromConfig(config)
      .load[T]
      .mapLeft(ConfigError(_))
      .toTry

  def extract[T](config: Config, path: String)(implicit reader: ConfigReader[T]): Try[T] =
    Try(config.getConfig(path)).flatMap(config => extract(config)(reader))
}
