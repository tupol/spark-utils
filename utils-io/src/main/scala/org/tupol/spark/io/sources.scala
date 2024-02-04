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
package org.tupol.spark.io

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.{ StringType, StructType }
import org.tupol.spark.Logging

package object sources {

  val ColumnNameOfCorruptRecord = "columnNameOfCorruptRecord"

  trait SourceConfiguration extends FormatAwareDataSourceConfiguration with Logging {

    /** The options the can be set to the `org.apache.spark.sql.DataFrameReader` */
    def options: Map[String, String]

    /** Add extra options and overwrites existing ones */
    def addOptions(extraOptions: Map[String, String]): SourceConfiguration

    /** The schema the can be set to the `org.apache.spark.sql.DataFrameReader` */
    def schema: Option[StructType]

    /** Set schema */
    def withSchema(schema: Option[StructType]): SourceConfiguration

    /** If the schema and columnNameOfCorruptRecord are defined add the columnNameOfCorruptRecord column to the schema */
    final def schemaWithCorruptRecord: Option[StructType] =
      (for {
        inputSchema <- schema
        errorColumn <- columnNameOfCorruptRecord
        _ = logDebug(
          s"The '$ColumnNameOfCorruptRecord' was specified; adding column '$errorColumn' to the input schema."
        )
        enhancedSchema = inputSchema.add(errorColumn, StringType)
      } yield enhancedSchema).orElse(schema)

    /** If the parser supports storing the failed records, they will be stored in this column */
    def columnNameOfCorruptRecord: Option[String] = options.get(ColumnNameOfCorruptRecord)
    override def toString: String = {
      val optionsStr = if (options.isEmpty) "" else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
      val schemaStr  = schema.map(_.prettyJson).getOrElse("not specified")
      s"format: '$format', options: {$optionsStr}, schema: $schemaStr"
    }
  }

  case class CsvSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Csv
    override def addOptions(extraOptions: Map[String, String]): CsvSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): CsvSourceConfiguration =
      this.copy(schema = schema)

    /** The csv parser does not support this feature */
    override val columnNameOfCorruptRecord = None
  }
  object CsvSourceConfiguration {
    def apply(
      options: Map[String, String],
      inputSchema: Option[StructType],
      delimiter: String,
      header: Boolean
    ): CsvSourceConfiguration =
      CsvSourceConfiguration(options + ("delimiter" -> delimiter) + ("header" -> header.toString), inputSchema)
    def apply(genericConfig: GenericSourceConfiguration, delimiter: String, header: Boolean): CsvSourceConfiguration =
      apply(genericConfig.options, genericConfig.schema, delimiter, header)
  }

  case class XmlSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Xml
    override def addOptions(extraOptions: Map[String, String]): XmlSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): XmlSourceConfiguration =
      this.copy(schema = schema)
  }
  object XmlSourceConfiguration {
    def apply(options: Map[String, String], inputSchema: Option[StructType], rowTag: String): XmlSourceConfiguration =
      XmlSourceConfiguration(options + ("rowTag" -> rowTag), inputSchema)
    def apply(genericConfig: GenericSourceConfiguration, rowTag: String): XmlSourceConfiguration =
      apply(genericConfig.options, genericConfig.schema, rowTag)
  }

  case class JsonSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Json
    override def addOptions(extraOptions: Map[String, String]): JsonSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): JsonSourceConfiguration =
      this.copy(schema = schema)
  }
  object JsonSourceConfiguration {
    def apply(basicConfig: GenericSourceConfiguration) =
      new JsonSourceConfiguration(basicConfig.options, basicConfig.schema)
  }

  case class ParquetSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Parquet
    override def addOptions(extraOptions: Map[String, String]): ParquetSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): ParquetSourceConfiguration =
      this.copy(schema = schema)
  }
  object ParquetSourceConfiguration {
    def apply(basicConfig: GenericSourceConfiguration) =
      new ParquetSourceConfiguration(basicConfig.options, basicConfig.schema)
  }

  case class OrcSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Orc
    override def addOptions(extraOptions: Map[String, String]): OrcSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): OrcSourceConfiguration =
      this.copy(schema = schema)
  }
  object OrcSourceConfiguration {
    def apply(basicConfig: GenericSourceConfiguration) =
      new OrcSourceConfiguration(basicConfig.options, basicConfig.schema)
  }

  case class AvroSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Avro
    override def addOptions(extraOptions: Map[String, String]): AvroSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): AvroSourceConfiguration =
      this.copy(schema = schema)
  }
  object AvroSourceConfiguration {
    def apply(basicConfig: GenericSourceConfiguration) =
      new AvroSourceConfiguration(basicConfig.options, basicConfig.schema)
  }

  case class TextSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Text
    override def addOptions(extraOptions: Map[String, String]): TextSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): TextSourceConfiguration =
      this.copy(schema = schema)
  }
  object TextSourceConfiguration {
    def apply(basicConfig: GenericSourceConfiguration) =
      new TextSourceConfiguration(basicConfig.options, basicConfig.schema)
  }

  /**
   * Basic configuration for the `JdbcDataSource`
   * @param url
   * @param table
   * @param user
   * @param password
   * @param driver
   */
  case class JdbcSourceConfiguration(options: Map[String, String] = Map(), schema: Option[StructType] = None)
      extends SourceConfiguration {
    val format = FormatType.Jdbc
    override def addOptions(extraOptions: Map[String, String]): JdbcSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): JdbcSourceConfiguration =
      this.copy(schema = schema)

    def table: String = options.get(JDBCOptions.JDBC_TABLE_NAME).getOrElse("")
    def url: String   = options.get(JDBCOptions.JDBC_URL).getOrElse("")

    override def toString: String = {
      val optionsStr =
        if (options.isEmpty) ""
        else options.map { case (k, v) => s"$k: '$v'" }.mkString(" ", ", ", " ")
      s"format: '$format', connection properties: {$optionsStr}"
    }
  }
  object JdbcSourceConfiguration {
    def apply(
      url: String,
      table: String,
      user: Option[String],
      password: Option[String],
      driver: Option[String],
      options: Map[String, String],
      schema: Option[StructType]
    ): JdbcSourceConfiguration = {
      val userOption     = user.map(v => Map("user"         -> v)).getOrElse(Nil)
      val passwordOption = password.map(v => Map("password" -> v)).getOrElse(Nil)
      val driverOption   = driver.map(v => Map("driver"     -> v)).getOrElse(Nil)
      val extraOptions = Map(JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table) ++
        userOption ++ passwordOption ++ driverOption
      new JdbcSourceConfiguration(options ++ extraOptions, schema)
    }
    def apply(
      url: String,
      table: String,
      user: String,
      password: String,
      driver: String,
      options: Map[String, String],
      schema: Option[StructType]
    ): JdbcSourceConfiguration =
      apply(url, table, Some(user), Some(password), Some(driver), options, schema)
    def apply(
      url: String,
      table: String,
      user: String,
      password: String,
      driver: String,
      options: Map[String, String]
    ): JdbcSourceConfiguration =
      apply(url, table, Some(user), Some(password), Some(driver), options, None)
    def apply(url: String, table: String, user: String, password: String, driver: String): JdbcSourceConfiguration =
      apply(url, table, Some(user), Some(password), Some(driver), Map[String, String](), None)
  }

  case class GenericSourceConfiguration(
    format: FormatType,
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None
  ) extends SourceConfiguration {
    override def addOptions(extraOptions: Map[String, String]): GenericSourceConfiguration =
      this.copy(options = this.options ++ extraOptions)
    override def withSchema(schema: Option[StructType]): GenericSourceConfiguration =
      this.copy(schema = schema)
  }
}
