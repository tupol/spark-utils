package org.tupol.spark.io.pureconf

import com.typesafe.config.ConfigRenderOptions
import org.apache.spark.sql.types.StructType
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.pureconf.errors.ConfigTryFailure
import org.tupol.spark.sql.loadSchemaFromString
import org.tupol.spark.utils.fuzzyLoadTextResourceFile
import pureconfig.{ConfigCursor, ConfigReader}
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import org.tupol.utils.implicits._

object readers {

  /**
   * Configuration extractor for Schemas.
   *
   * It can be used as
   * `config.extract[Option[StructType]]("configuration_path_to_schema")` or as
   * `config.extract[StructType]("configuration_path_to_schema")`
   */
  val StructTypeReader: ConfigReader[StructType] = ConfigReader.fromCursor[StructType] { cur =>
    val pathKey = "path"
    def fromPath(cur: ConfigCursor): ConfigReader.Result[StructType] =
      for {
        objCur  <- cur.asObjectCursor
        pathCur <- objCur.atKey(pathKey)
        path    <- pathCur.asString
        stringSchema <- fuzzyLoadTextResourceFile(path).toEither
          .mapLeft(t => ConfigReaderFailures(ConfigTryFailure(t, objCur.origin, s"${cur.path}.$pathKey")))
        schema <- loadSchemaFromString(stringSchema).toEither
          .mapLeft(t => ConfigReaderFailures(ConfigTryFailure(t, objCur.origin, s"${cur.path}.$pathKey")))
      } yield schema

    def fromString(cur: ConfigCursor): ConfigReader.Result[StructType] = {
      for {
        objCur <- cur.asConfigValue
        stringSchema = objCur.render(ConfigRenderOptions.concise())
        schema <- loadSchemaFromString(stringSchema).toEither
          .mapLeft(t => ConfigReaderFailures(ConfigTryFailure(t, cur.origin, cur.path)))
      } yield schema
    }

    fromString(cur) match {
      case Left(fx) => fromPath(cur).mapLeft(_ ++ fx)
      case res      => res
    }
  }
  val FormatTypeReader: ConfigReader[FormatType] = ConfigReader[String].emap(value => FormatType.fromString(value).toEither.mapLeft(t => CannotConvert(value, "FormatType", t.getMessage)))

}
