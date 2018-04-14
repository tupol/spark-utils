package io.tupol.spark

import java.io.File

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.SparkSession
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Try

class SparkRunnableSpec extends FunSuite with Matchers with SharedSparkSession {

  val filesArg = Seq(
    new File("./src/test/resources/MockRunnable/application.conf").getAbsolutePath
  )

  override def sparkConfig: Map[String, String] = {
    // Add the comma separated configuration files to the files property.
    // There can be just one file with the same name, as they all end up at the same level in the same folder.
    // There is an exception however, if the files have the same content no exception will be thrown.
    super.sparkConfig +
      ("spark.files" -> filesArg.mkString(","))
  }

  test(
    """SparkRunnable.applicationConfiguration loads first the app params then defaults to application.conf file,
      |then to the application.conf in the classpath and then to reference.conf""".stripMargin
  ) {

    val conf = MockRunnable.applicationConfiguration(spark)(Array(
      "MockRunnable.whoami=\"app.param\"",
      "MockRunnable.param=\"param\""
    ))
    conf.getString("param") shouldBe "param"
    conf.getString("whoami") shouldBe "app.param"
    conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
    conf.getString("reference") shouldBe "reference"
    conf.getBoolean("file.application.conf") shouldBe true
  }

  test("SparkRunnable.applicationConfiguration loads first the application.conf then defaults to reference.conf") {
    val conf = MockRunnable.applicationConfiguration(spark)(Array(
      "MockRunnable.param=\"param\""
    ))
    conf.getString("param") shouldBe "param"
    conf.getString("whoami") shouldBe "./src/test/resources/MockRunnable/application.conf"
    conf.getStringList("some.list").toArray shouldBe Seq("a", "b", "c")
    conf.getString("reference") shouldBe "reference"
    conf.getBoolean("file.application.conf") shouldBe true
  }

  test("SparkRunnable.applicationConfiguration ignores specified paths when logs config") {

    val runnable = new SparkRunnable[String, Unit] {

      override val skippedPathsWhenLogging: Seq[String] = Seq("app.app1.credentials", "app.app1.password")

      override def buildConfig(config: Config): Try[String] = ???
      override def run(spark: SparkSession, config: String): Try[Unit] = ???

      def loggedConfig(config: Config): String = renderConfig(config)
    }

    val conf = ConfigFactory.parseString(
      """
        |app.app1.credentials: {
        |    user = "user1"
        |    password = "******"
        |}
        |
        |app.app1.password = "qwerty"
        |
        |app.app1.nobody.needs.it: {
        |    something = "stuff"
        |}
      """.stripMargin
    )

    import scala.language.reflectiveCalls
    val actual = runnable.loggedConfig(conf)

    val expected =
      """{
        |    # String: 2-9
        |    "app" : {
        |        # String: 2-9
        |        "app1" : {
        |            # String: 9
        |            "nobody" : {
        |                # String: 9
        |                "needs" : {
        |                    # String: 9
        |                    "it" : {
        |                        # String: 10
        |                        "something" : "stuff"
        |                    }
        |                }
        |            }
        |        }
        |    }
        |}
        |""".stripMargin

    actual shouldEqual expected

  }

  object MockRunnable extends SparkRunnable[String, Unit] {

    def buildConfig(config: Config): Try[String] = Try("Hello")

    // This method needs to be implemented and should contain the entire runnable logic.
    override def run(spark: SparkSession, config: String): Try[Unit] = ???
  }
}

