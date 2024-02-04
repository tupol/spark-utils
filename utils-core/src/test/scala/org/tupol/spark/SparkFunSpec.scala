package org.tupol.spark

import java.io.File
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.{ Failure, Success, Try }

class SparkFunSpec extends AnyFunSuite with Matchers with LocalSparkSession {

  val filesArg = Seq(new File("src/test/resources/MockFun/fun.conf").getAbsolutePath)

  override def sparkConfig: Map[String, String] =
    // Add the comma separated configuration files to the files property.
    // There can be just one file with the same name, as they all end up at the same level in the same folder.
    // There is an exception however, if the files have the same content no exception will be thrown.
    super.sparkConfig +
      ("spark.files" -> filesArg.mkString(","))

  test("SparkFun.main successfully completes") {
    noException shouldBe thrownBy(MockFun$.main(Array()))
  }

  test("SparkFun.main successfully completes with no configuration expected") {
    noException shouldBe thrownBy(MockFunNoConfig.main(Array()))
  }

  test("SparkFun.main fails gracefully if SparkFun.run fails") {
    a[MockApException] shouldBe thrownBy(MockFunFailure.main(Array()))
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
