package org.tupol.spark.implicits

import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.functions.{col, struct}
import org.tupol.spark.encoders

import java.util.UUID

object dataset {

  implicit class DatasetOps[T: Encoder](val dataset: Dataset[T]) extends Serializable {

    import org.apache.spark.sql.{ Column, Dataset, Encoder }

    /**
     * Add a column to a dataset resulting in a dataset of a tuple of the type contained in the input dataset and the new column
     *
     * Sample usage:
     * {{{
     *   import org.tupol.spark.implicits._
     *   import org.apache.spark.sql.functions.lit
     *   import org.apache.spark.sql.Dataset
     *
     *   val dataset: Dataset[MyClass] = ...
     *   val datasetWithCol: Dataset[(MyClass, String)] = dataset.withColumnDataset[String](lit("some text"))
     * }}}
     *
     * @param column the column to be added
     * @tparam U The type of the added column
     * @return a Dataset containing a tuple of the input data and the given column
     */
    def withColumnDataset[U: Encoder](column: Column): Dataset[(T, U)] = {
      implicit val tuple2Encoder: Encoder[(T, U)] = encoders.tuple2[T, U]
      val tempColName = s"temp_col_${UUID.randomUUID()}"

      val tuple1 =
        if (dataset.encoder.clsTag.runtimeClass.isPrimitive) dataset.columns.map(col).head
        else struct(dataset.columns.map(col): _*)

      dataset
        .withColumn(tempColName, column)
        .select(tuple1 as "_1", col(tempColName) as "_2")
        .as[(T, U)]
    }
  }


  implicit class KeyValueDatasetOps[K: Encoder, V: Encoder](val dataset: Dataset[(K, V)]) extends Serializable {

    /**
     * Map values of a Dataset containing a key-value pair in a Tuple2
     *
     * Sample usage:
     * {{{
     *   import org.tupol.spark.implicits._
     *   import org.apache.spark.sql.Dataset
     *
     *   val dataset: Dataset[(String, Int)] = ...
     *   val result: Dataset[(String, Int)]  = dataset.mapValues(_ * 10)
     * }}}
     */
    def mapValues[U: Encoder](f: V => U): Dataset[(K, U)] = {
      implicit val tuple2Encoder: Encoder[(K, U)] = encoders.tuple2[K, U]
      dataset.map { case (k, v) => (k, f(v)) }
    }

    /**
     * FlatMap values of a Dataset containing a key-value pair in a Tuple2
     *
     * Sample usage:
     * {{{
     *   import org.tupol.spark.implicits._
     *   import org.apache.spark.sql.Dataset
     *
     *   val dataset: Dataset[(String, Int)] = ...
     *   val result: Dataset[(String, Int)]  = dataset.flatMapValues(Seq(1, 2, 3))
     * }}}
     */
    def flatMapValues[U: Encoder](f: V => TraversableOnce[U]): Dataset[(K, U)] = {
      implicit val tuple2Encoder: Encoder[(K, U)] = encoders.tuple2[K, U]
      dataset.flatMap { case (k, v) => f(v).map((k, _)) }
    }
  }

}
