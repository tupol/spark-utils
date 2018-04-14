package io.tupol.spark

import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterAll, Suite }

/**
 * Shares a SparkSession, with the SparkContext and the SqlContext for all the tests in the Suite.
 */
trait SharedSparkSession extends BeforeAndAfterAll {
  this: Suite =>

  def master = "local[*]"
  def appName = this.getClass.getSimpleName

  @transient private var _spark: SparkSession = _

  lazy val spark: SparkSession = _spark

  lazy val sc: SparkContext = spark.sparkContext

  lazy val sqlContext: SQLContext = spark.sqlContext

  def sparkConfig: Map[String, String] = Map.empty

  override def beforeAll(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    super.beforeAll()

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    _spark = SparkSession.builder.config(conf).getOrCreate()

  }

  override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark = null
      }
    } finally {
      super.afterAll()
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }

}
