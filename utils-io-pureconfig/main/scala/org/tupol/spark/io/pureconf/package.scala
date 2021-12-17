package org.tupol.spark.io

import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import pureconfig.ConfigReader


package object pureconf {

  implicit class ConfigOps(config: Config) extends org.tupol.spark.io.pureconf.config.ConfigOps(config)
  implicit val StructTypeReader: ConfigReader[StructType] = readers.StructTypeReader
  implicit val FormatTypeReader: ConfigReader[FormatType] = readers.FormatTypeReader

}
