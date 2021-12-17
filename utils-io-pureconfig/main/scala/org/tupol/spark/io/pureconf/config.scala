package org.tupol.spark.io.pureconf

import com.typesafe.config.Config
import org.tupol.spark.io.pureconf.errors.ConfigError
import pureconfig.{ConfigReader, ConfigSource}
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
    import org.tupol.spark.io.pureconf.{config => packageConf}
    def extract[T](implicit reader: ConfigReader[T]): Try[T] = packageConf.extract[T](config)
    def extract[T](path: String)(implicit reader: ConfigReader[T]): Try[T] = packageConf.extract[T](config, path)
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
    Try(config.getConfig(path)).flatMap(config => extract(config))
}
