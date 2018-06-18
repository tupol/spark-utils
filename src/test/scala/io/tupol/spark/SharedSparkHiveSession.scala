package io.tupol.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

/**
 * Shares a SparkSession having the Hive support enabled, with the SparkContext and the SqlContext for all the tests in the Suite.
 */
trait SharedSparkHiveSession extends SharedSparkSession {
  this: Suite =>

  override def createSparkSession(conf: SparkConf): SparkSession =
    SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()

}
