package org.tupol.spark.implicits

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

object map {

  /** Map decorator with a Spark flavour */
  implicit class MapOps[K, V](val map: Map[K, V]) extends AnyRef {
    // Primitive conversion from Map to a Row using a schema
    // TODO: Deal with inner maps
    def toRow(schema: StructType): Row = {
      val values = schema.fieldNames.map(fieldName => map.map { case (k, v) => (k.toString(), v) }.get(fieldName).getOrElse(null))
      new GenericRowWithSchema(values, schema)
    }
  }

}
