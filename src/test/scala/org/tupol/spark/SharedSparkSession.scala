package org.tupol.spark

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.util.Try

/**
 * Shares a SparkSession, with the SparkContext and the SqlContext for all the tests in the Suite.
 */
trait SharedSparkSession extends BeforeAndAfterAll {
  this: Suite =>

  def master = "local[2]"
  def appName = this.getClass.getSimpleName

  val TempDir = Option(System.getProperty("java.io.tmpdir")).getOrElse("/tmp")

  @transient private var _spark: SparkSession = _

  implicit lazy val spark: SparkSession = _spark

  implicit lazy val sc: SparkContext = spark.sparkContext

  implicit lazy val sqlContext: SQLContext = spark.sqlContext

  private var _sparkConfig: Map[String, String] = _

  def sparkConfig: Map[String, String] = Map(
    "spark.driver.host" -> "localhost" //See https://issues.apache.org/jira/browse/SPARK-19394
  )

  def createSparkSession(conf: SparkConf): SparkSession =
    SparkSession.builder.config(conf).getOrCreate()

  override def beforeAll(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    super.beforeAll()

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }
    def warehouseDirName = s"$TempDir/spark-warehouse-${UUID.randomUUID().toString}.test.temp"
    def warehouseDirPath = new java.io.File(warehouseDirName).getAbsolutePath
    conf.set("spark.sql.warehouse.dir", warehouseDirPath)

    _spark = createSparkSession(conf)

  }

  override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        Try(_spark.close())
        _spark = null
      }
      sparkConfig.get("spark.sql.warehouse.dir")
        .map(name => FileUtils.deleteQuietly(new File(name)))
    } finally {
      super.afterAll()
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }

}
