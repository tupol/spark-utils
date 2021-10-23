package org.tupol.spark.implicits

import org.apache.spark.sql.Row
import org.tupol.spark.sql.row2map

object row {

  /** Row decorator. */
  implicit class RowOps(val row: Row) extends AnyRef {
    // Primitive conversion from Map to a Row using a schema
    // TODO: this does not take care of inner maps
    def toMap: Map[String, Any] = row2map(row)
  }

}
