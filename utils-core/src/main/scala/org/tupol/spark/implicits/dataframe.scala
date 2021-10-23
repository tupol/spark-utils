package org.tupol.spark.implicits

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tupol.spark.sql

object dataframe {

  /** DataFrame decorator. */
  implicit class DataFrameOps(val dataFrame: DataFrame) extends AnyRef{

    /** See [[org.tupol.spark.sql.flattenFields()]] */
    def flattenFields: DataFrame = sql.flattenFields(dataFrame)

    /** Not all column names are compliant to the Avro format. This function renames to columns to be Avro compliant */
    def makeAvroCompliant(implicit spark: SparkSession): DataFrame =
      sql.makeDataFrameAvroCompliant(dataFrame)
  }

}
