package org.tupol.spark

import java.io.File
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.{ FunSuite, Matchers }

import scala.util.{ Failure, Success, Try }

class SparkFunSpec extends FunSuite with Matchers with LocalSparkSession {

  val filesArg = Seq(
    new File("src/test/resources/MockFun/application.conf").getAbsolutePath)

  override def sparkConfig: Map[String, String] = {
    // Add the comma separated configuration files to the files property.
    // There can be just one file with the same name, as they all end up at the same level in the same folder.
    // There is an exception however, if the files have the same content no exception will be thrown.
    super.sparkConfig +
      ("spark.files" -> filesArg.mkString(","))
  }

  test(
    """SparkFun.applicationConfiguration loads first the app params then defaults to application.conf file,
      |then to the application.conf in the classpath and then to reference.conf""".stripMargin) {

      val conf = MockFun$.applicationConfiguration(spark, Array(
        "MockFun.whoami=\"app.param\"",
        "MockFun.param=\"param\""))
      conf.getString("param") shouldBe "param"
      conf.getString("whoami") shouldBe "app.param"
      conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
      conf.getString("reference") shouldBe "reference_mock_fun"
      conf.getBoolean("file.application.conf") shouldBe true
    }

  test("SparkFun.applicationConfiguration loads first the application.conf then defaults to reference.conf") {
    val conf = MockFun$.applicationConfiguration(spark, Array(
      "MockFun.param=\"param\""))
    conf.getString("param") shouldBe "param"
    conf.getString("whoami") shouldBe "./src/test/resources/MockFun/application.conf"
    conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
    conf.getString("reference") shouldBe "reference_mock_fun"
    conf.getBoolean("file.application.conf") shouldBe true
  }

  test("SparkFun.applicationConfiguration performs variable substitution") {
    val conf = MockFun$.applicationConfiguration(spark, Array(
      "MockFun.param=\"param\"", "my.var=\"MYVAR\""))
    conf.getString("param") shouldBe "param"
    conf.getString("whoami") shouldBe "./src/test/resources/MockFun/application.conf"
    conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
    conf.getString("reference") shouldBe "reference_mock_fun"
    conf.getString("substitute.my-var") shouldBe "MYVAR"
    conf.getString("substitute.my-other-var") shouldBe "MYVAR"
    conf.getBoolean("file.application.conf") shouldBe true
  }

  test("SparkFun.main successfully completes") {
    noException shouldBe thrownBy(MockFun$.main(Array()))
    spark.sparkContext.isStopped shouldBe true
  }

  test("SparkFun.main successfully completes with no configuration expected") {
    noException shouldBe thrownBy(MockFunNoConfig.main(Array()))
    spark.sparkContext.isStopped shouldBe true
  }

  test("SparkFun.main fails gracefully if SparkFun.run fails") {
    a[MockApException] shouldBe thrownBy(MockFunFailure.main(Array()))
    spark.sparkContext.isStopped shouldBe true
  }

  test("SparkFun.appName gets the simple class name") {
    MockFun$.appName shouldBe "MockFun"
    MockFunFailure.appName shouldBe "MockFunFailure"
  }

  object MockFunConfig extends (Config => Try[String]) {
    def apply(config: Config): Try[String] = Success("Hello")
  }

  val contextFactory: Config => Try[String] = (_: Config) => Success("Hello")

  object MockFun$ extends SparkFun[String, Unit](MockFunConfig(_)) {
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockFunNoConfig extends SparkFun[String, Unit](contextFactory) {
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockFunFailure extends SparkFun[String, Unit]((_: Config) => Try("Hello")) {
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Failure(new MockApException)
  }

  class MockApException extends Exception

}
