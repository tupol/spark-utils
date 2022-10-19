package org.tupol.spark

import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterEach, Suite }

import scala.util.Try

/** A fresh SparkSession, with the SparkContext and the SqlContext for each test in the Suite. */
trait LocalSparkSession extends BeforeAndAfterEach {
  this: Suite =>

  def master = "local[2]"
  def appName = this.getClass.getSimpleName

  @transient private var _spark: SparkSession = _

  implicit lazy val spark: SparkSession = _spark

  implicit lazy val sc: SparkContext = spark.sparkContext

  implicit lazy val sqlContext: SQLContext = spark.sqlContext

  def sparkConfig: Map[String, String] = Map("spark.driver.host" -> "localhost") //See https://issues.apache.org/jira/browse/SPARK-19394

  def createSparkSession(conf: SparkConf): SparkSession =
    SparkSession.builder.config(conf).getOrCreate()

  override def beforeEach(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    super.beforeEach()

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    _spark = createSparkSession(conf)

  }

  override def afterEach(): Unit = {
    try {
      if (_spark != null) {
        Try(_spark.close())
        _spark = null
      }
    } finally {
      super.afterEach()
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }

}
