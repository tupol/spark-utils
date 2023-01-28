package org.tupol.spark

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.apache.spark.sql.types.StructType
import org.tupol.spark.implicits.dataframe._
import org.tupol.spark.implicits.dataset._
import org.tupol.spark.implicits.schema._
import org.tupol.spark.implicits.map._
import org.tupol.spark.implicits.row._


package object implicits {

  implicit class RowOpsImplicits(override val row: Row) extends RowOps(row)

  implicit class MapOpsImplicits[K, V](override val map: Map[K, V]) extends MapOps[K, V](map)

  implicit class SchemaOpsImplicits(override val schema: StructType) extends SchemaOps(schema)

  implicit class DataFrameOpsImplicits(override val dataFrame: DataFrame) extends DataFrameOps(dataFrame)

  implicit class DatasetOpsImplicits[T: Encoder](override val dataset: Dataset[T]) extends DatasetOps[T](dataset)

  implicit class KeyValueDatasetOpsImplicits[K: Encoder, V: Encoder](override val dataset: Dataset[(K, V)]) extends KeyValueDatasetOps[K, V](dataset)

}
