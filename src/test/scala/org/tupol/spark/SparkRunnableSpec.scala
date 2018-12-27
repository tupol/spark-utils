package org.tupol.spark

import java.io.File

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Try

class SparkRunnableSpec extends FunSuite with Matchers with SharedSparkSession {

  val filesArg = Seq(
    new File("src/test/resources/MockRunnable/application.conf").getAbsolutePath)

  override def sparkConfig: Map[String, String] = {
    // Add the comma separated configuration files to the files property.
    // There can be just one file with the same name, as they all end up at the same level in the same folder.
    // There is an exception however, if the files have the same content no exception will be thrown.
    super.sparkConfig +
      ("spark.files" -> filesArg.mkString(","))
  }

  test(
    """SparkRunnable.applicationConfiguration loads first the app params then defaults to application.conf file,
      |then to the application.conf in the classpath and then to reference.conf""".stripMargin) {

      val conf = MockRunnable.applicationConfiguration(spark, Array(
        "MockRunnable.whoami=\"app.param\"",
        "MockRunnable.param=\"param\""))
      conf.getString("param") shouldBe "param"
      conf.getString("whoami") shouldBe "app.param"
      conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
      conf.getString("reference") shouldBe "reference"
      conf.getBoolean("file.application.conf") shouldBe true
    }

  test("SparkRunnable.applicationConfiguration loads first the application.conf then defaults to reference.conf") {
    val conf = MockRunnable.applicationConfiguration(spark, Array(
      "MockRunnable.param=\"param\""))
    conf.getString("param") shouldBe "param"
    conf.getString("whoami") shouldBe "./src/test/resources/MockRunnable/application.conf"
    conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
    conf.getString("reference") shouldBe "reference"
    conf.getBoolean("file.application.conf") shouldBe true
  }

  object MockRunnable extends SparkRunnable[String, Unit] {

    def buildConfig(config: Config): Try[String] = Try("Hello")

    // This method needs to be implemented and should contain the entire runnable logic.
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = ???
  }
}

