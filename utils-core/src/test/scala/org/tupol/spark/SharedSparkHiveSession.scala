package org.tupol.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

/**
 * Shares a SparkSession having the Hive support enabled, with the SparkContext and the SqlContext for all the tests in the Suite.
 */
trait SharedSparkHiveSession extends SharedSparkSession {
  this: Suite =>

  override def sparkSessionBuilder: SparkSession.Builder = super.sparkSessionBuilder
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", warehouseDirPath)

}
