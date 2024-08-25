package org.tupol.spark

import java.io.File
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.{ Failure, Success, Try }

class SparkFunSpec extends AnyFunSuite with Matchers with LocalSparkSession {

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

  object MockFunConfig extends (Array[String] => Try[String]) {
    def apply(args: Array[String]): Try[String] = Success("Hello")
  }

  val contextFactory: Array[String] => Try[String] = (_: Array[String]) => Success("Hello")

  object MockFun$ extends SparkFun[String, Unit](MockFunConfig(_)) {
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockFunNoConfig extends SparkFun[String, Unit](contextFactory) {
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockFunFailure extends SparkFun[String, Unit]((_: Array[String]) => Try("Hello")) {
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Failure(new MockApException)
  }

  class MockApException extends Exception

}
