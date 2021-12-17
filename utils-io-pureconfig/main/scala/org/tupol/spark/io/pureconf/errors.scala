package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigOrigin
import pureconfig.error.{ConfigReaderException, ConfigReaderFailure, ConfigReaderFailures}

object errors {

  case class ConfigError(message: String) extends Exception(message)
  object ConfigError {
    def apply(configReaderFailures: ConfigReaderFailures): ConfigError =
      ConfigError(ConfigReaderException(configReaderFailures).getMessage())
    def apply(message: String): ConfigError =
      new ConfigError(message)
  }

}
