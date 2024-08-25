package org.tupol.spark.app

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.LocalSparkSession
import org.tupol.spark.app.config.FuzzyTypesafeConfigBuilder

import java.io.File
import scala.util.{Failure, Success, Try}

class SparkAppSpec extends AnyFunSuite with Matchers with LocalSparkSession {

  val configurationFileName = "app.conf"
  val filesArg              = Seq(new File("src/test/resources/MockApp/app.conf").getAbsolutePath)

  override def sparkConfig: Map[String, String] =
    // Add the comma separated configuration files to the files property.
    // There can be just one file with the same name, as they all end up at the same level in the same folder.
    // There is an exception however, if the files have the same content no exception will be thrown.
    super.sparkConfig +
      ("spark.files" -> filesArg.mkString(","))

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
    def createContext(config: Config): Try[String]                            = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())
  }

  object MockAppNoConfig extends SparkApp[String, Unit] {
    def createContext(config: Config): Try[String]                            = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Success(())

    override def loadConfiguration(args: Seq[String], configurationFileName: String): Try[Config] =
      FuzzyTypesafeConfigBuilder.loadConfiguration(args: Seq[String], configurationFileName: String)
  }

  object MockAppFailure extends SparkApp[String, Unit] {
    def createContext(config: Config): Try[String]                            = Success("Hello")
    override def run(implicit spark: SparkSession, config: String): Try[Unit] = Failure(new MockApException)
  }

  class MockApException extends Exception

}
