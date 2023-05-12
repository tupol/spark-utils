package org.tupol.spark

import java.util.UUID

import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.util.Try

/** Shares a SparkSession, with the SparkContext and the SqlContext for all the tests in the Suite. */
trait SharedSparkSession extends BeforeAndAfterAll {
  this: Suite =>

  def master = "local[2]"
  def appName = simpleClassName(this)

  val TempDir = Option(System.getProperty("java.io.tmpdir")).getOrElse("/tmp")

  def sparkConfig: Map[String, String] = Map()

  def warehouseDirName = s"$TempDir/spark-warehouse-${UUID.randomUUID().toString}.test.temp"

  def warehouseDirPath = new java.io.File(warehouseDirName).getAbsolutePath

  private def defaultSparkSessionBuilder = SparkSession.builder()
    .appName(appName)
    .master(master)
    .config("spark.driver.host", "localhost")

  def sparkSessionBuilder: SparkSession.Builder = sparkConfig
    .foldLeft(defaultSparkSessionBuilder)((acc, kv) => acc.config(kv._1, kv._2))

  implicit lazy val spark: SparkSession = sparkSessionBuilder.getOrCreate()

  implicit lazy val sc: SparkContext = spark.sparkContext

  implicit lazy val sqlContext: SQLContext = spark.sqlContext


  override def afterAll(): Unit = {
    Try(super.afterAll())
    if (spark != null) {
      Try(spark.sessionState.catalog.reset())
      Try(spark.close())
      Try(SparkSession.clearActiveSession())
      Try(SparkSession.clearDefaultSession())
    }
  }

}
