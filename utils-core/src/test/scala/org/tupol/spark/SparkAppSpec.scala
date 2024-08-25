package org.tupol.spark

import java.io.File
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.{ Failure, Success, Try }

class SparkAppSpec extends AnyFunSuite with Matchers with LocalSparkSession {

  test("SparkApp.main successfully completes") {
    noException shouldBe thrownBy(MockApp$.main(Array()))
  }

  test("SparkApp.main successfully completes with no configuration expected") {
    noException shouldBe thrownBy(MockAppNoConfig.main(Array()))
  }

  test("SparkApp.main fails gracefully if SparkApp.run fails") {
    a[MockApException] shouldBe thrownBy(MockAppFailure.main(Array()))
  }

  test("SparkApp.appName gets the simple class name") {
    MockApp$.appName shouldBe "MockApp"
    MockAppFailure.appName shouldBe "MockAppFailure"
  }

  object MockApp$ extends SparkApp[String, Unit] {
    def createContext(args: Array[String]): Try[String]                            = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockAppNoConfig extends SparkApp[String, Unit] {
    def createContext(args: Array[String]): Try[String]                            = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockAppFailure extends SparkApp[String, Unit] {
    def createContext(args: Array[String]): Try[String]                            = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Failure(new MockApException)
  }

  class MockApException extends Exception

}
