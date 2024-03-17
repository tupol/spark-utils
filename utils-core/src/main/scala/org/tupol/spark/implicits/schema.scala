package org.tupol.spark.implicits

import org.apache.spark.sql.types.{ StructField, StructType }
import org.tupol.spark.sql

object schema {

  /** StructType decorator. */
  implicit class SchemaOps(val schema: StructType) extends AnyRef {

    /** See `org.tupol.spark.sql.mapFields()` */
    def mapFields(mapFun: StructField => StructField): StructType =
      sql.mapFields(schema, mapFun).asInstanceOf[StructType]

    /** See `org.tupol.spark.sql.checkAllFields()` */
    def checkAllFields(predicate: StructField => Boolean): Boolean = sql.checkAllFields(schema, predicate)

    /** See `org.tupol.spark.sql.checkAnyFields()` */
    def checkAnyFields(predicate: StructField => Boolean): Boolean = sql.checkAnyFields(schema, predicate)
  }

}
