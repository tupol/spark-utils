package org.tupol.spark

import java.io.File
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.{ FunSuite, Matchers }

import scala.util.{ Failure, Success, Try }

class SparkAppSpec extends FunSuite with Matchers with LocalSparkSession {

  val filesArg = Seq(
    new File("src/test/resources/MockApp/application.conf").getAbsolutePath)

  override def sparkConfig: Map[String, String] = {
    // Add the comma separated configuration files to the files property.
    // There can be just one file with the same name, as they all end up at the same level in the same folder.
    // There is an exception however, if the files have the same content no exception will be thrown.
    super.sparkConfig +
      ("spark.files" -> filesArg.mkString(","))
  }

  test(
    """SparkApp.applicationConfiguration loads first the app params then defaults to application.conf file,
      |then to the application.conf in the classpath and then to reference.conf""".stripMargin) {

      val conf = MockApp$.applicationConfiguration(spark, Array(
        "MockApp.whoami=\"app.param\"",
        "MockApp.param=\"param\""))
      conf.getString("param") shouldBe "param"
      conf.getString("whoami") shouldBe "app.param"
      conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
      conf.getString("reference") shouldBe "reference_mock_app"
      conf.getBoolean("file.application.conf") shouldBe true
    }

  test("SparkApp.applicationConfiguration loads first the application.conf then defaults to reference.conf") {
    val conf = MockApp$.applicationConfiguration(spark, Array(
      "MockApp.param=\"param\""))
    conf.getString("param") shouldBe "param"
    conf.getString("whoami") shouldBe "./src/test/resources/MockApp/application.conf"
    conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
    conf.getString("reference") shouldBe "reference_mock_app"
    conf.getBoolean("file.application.conf") shouldBe true
  }

  test("SparkApp.applicationConfiguration performs variable substitution") {
    val conf = MockApp$.applicationConfiguration(spark, Array(
      "MockApp.param=\"param\"", "my.var=\"MYVAR\""))
    conf.getString("param") shouldBe "param"
    conf.getString("whoami") shouldBe "./src/test/resources/MockApp/application.conf"
    conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
    conf.getString("reference") shouldBe "reference_mock_app"
    conf.getString("substitute.my-var") shouldBe "MYVAR"
    conf.getString("substitute.my-other-var") shouldBe "MYVAR"
    conf.getBoolean("file.application.conf") shouldBe true
  }

  test("SparkApp.main successfully completes") {
    noException shouldBe thrownBy(MockApp$.main(Array()))
    spark.sparkContext.isStopped shouldBe true
  }

  test("SparkApp.main successfully completes with no configuration expected") {
    noException shouldBe thrownBy(MockAppNoConfig.main(Array()))
    spark.sparkContext.isStopped shouldBe true
  }

  test("SparkApp.main fails gracefully if SparkApp.run fails") {
    a[MockApException] shouldBe thrownBy(MockAppFailure.main(Array()))
    spark.sparkContext.isStopped shouldBe true
  }

  test("SparkApp.appName gets the simple class name") {
    MockApp$.appName shouldBe "MockApp"
    MockAppFailure.appName shouldBe "MockAppFailure"
  }

  object MockApp$ extends SparkApp[String, Unit] {
    def createContext(config: Config): Try[String] = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockAppNoConfig extends SparkApp[String, Unit] {
    def createContext(config: Config): Try[String] = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockAppFailure extends SparkApp[String, Unit] {
    def createContext(config: Config): Try[String] = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Failure(new MockApException)
  }

  class MockApException extends Exception

}

