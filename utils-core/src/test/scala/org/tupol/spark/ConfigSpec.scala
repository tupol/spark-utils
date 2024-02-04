package org.tupol.spark

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.config.{FuzzyTypesafeConfigBuilder, SimpleTypesafeConfigBuilder}

import java.io.File
import scala.util.Try

class ConfigSpec extends AnyWordSpec with Matchers with LocalSparkSession {

  val filesArg = Seq(
    new File("src/test/resources/MockApp/app.conf").getAbsolutePath,
    new File("src/test/resources/MockFun/fun.conf").getAbsolutePath
  )

  override def sparkConfig: Map[String, String] = {
    // Add the comma separated configuration files to the files property.
    // There can be just one file with the same name, as they all end up at the same level in the same folder.
    // There is an exception however, if the files have the same content no exception will be thrown.
    super.sparkConfig +
      ("spark.files" -> filesArg.mkString(","))
  }

  "FuzzyTypesafeConfigBuilder.getConfiguration" should {
    "load first the app params then defaults to the Spark app.conf file, then to the app.conf in the classpath and then to reference.conf" in {
      val configBuilder = FuzzyTypesafeConfigBuilder
      val conf = configBuilder.loadConfiguration(Array(
        "MockApp.whoami=\"app.param\"",
        "MockApp.param=\"param\""), "app.conf").get

      conf.getString("MockApp.param") shouldBe "param"
      conf.getString("MockApp.whoami") shouldBe "app.param"
      conf.getStringList("MockApp.some.list").toArray shouldBe Seq("a", "b", "c")
      conf.getString("MockApp.reference") shouldBe "reference_mock_app"
      conf.getBoolean("MockApp.file.application.conf") shouldBe true
    }
    "load first the app.conf then defaults to reference.conf" in {
      val configBuilder = FuzzyTypesafeConfigBuilder
      val conf = configBuilder.loadConfiguration(Array("MockFun.param=\"param\""), "fun.conf").get
      conf.getString("MockFun.param") shouldBe "param"
      conf.getString("MockFun.whoami") shouldBe "./src/test/resources/MockFun/fun.conf"
      conf.getStringList("MockFun.some.list").toArray shouldBe Seq("a", "b", "c")
      conf.getString("MockFun.reference") shouldBe "reference_mock_fun"
      conf.getBoolean("MockFun.file.application.conf") shouldBe true
    }
    "perform variable substitution" in {
      val configBuilder = FuzzyTypesafeConfigBuilder
      val conf = configBuilder.loadConfiguration(Array(
        "MockApp.param=\"param\"", "my.var=\"MYVAR\""), "app.conf").get
      conf.getString("MockApp.param") shouldBe "param"
      conf.getString("MockApp.whoami") shouldBe "./src/test/resources/MockApp/app.conf"
      conf.getStringList("MockApp.some.list").toArray shouldBe Seq("a", "b", "c")
      conf.getString("MockApp.reference") shouldBe "reference_mock_app"
      conf.getString("MockApp.substitute.my-var") shouldBe "MYVAR"
      conf.getString("MockApp.substitute.my-other-var") shouldBe "MYVAR"
      conf.getBoolean("MockApp.file.application.conf") shouldBe true
    }
    "perform variable substitution and addition" in {
      val configBuilder = FuzzyTypesafeConfigBuilder
      val conf = configBuilder.loadConfiguration(Array(
        "MockApp.param=param", "MockApp.my.var=MYVAR", "MockApp.substitute.my-other-var=MY_OTHER_VAR"), "MockApp/app2.conf").get
      conf.getString("MockApp.param") shouldBe "param"
      conf.getString("MockApp.whoami") shouldBe "./src/test/resources/MockApp/app2.conf"
      conf.getStringList("MockApp.some.list").toArray shouldBe Seq("a", "b", "c")
      conf.getString("MockApp.reference") shouldBe "reference_mock_app"
      conf.getString("MockApp.substitute.my-var") shouldBe "MYVAR"
      conf.getString("MockApp.substitute.my-other-var") shouldBe "MY_OTHER_VAR"
      conf.getBoolean("MockApp.file.application.conf") shouldBe true
    }
  }

  "SimpleTypesafeConfigBuilder" should {
    "loads first the app params then defaults to app.conf file, then to the app.conf in the classpath and then to reference.conf" in {
      val configBuilder = SimpleTypesafeConfigBuilder
      val conf = configBuilder.loadConfiguration(Array(
        "MockApp.whoami=\"app.param\"",
        "MockApp.param=\"param\""), "fun.conf").get

      conf.getString("MockApp.param") shouldBe "param"
      conf.getString("MockApp.whoami") shouldBe "app.param"
      conf.getString("MockApp.reference") shouldBe "reference_mock_app"
      Try(conf.getStringList("MockApp.some.list")).toOption shouldBe None
      Try(conf.getBoolean("MockApp.file.application.conf")).toOption shouldBe None
    }
    "defaults to reference.conf" in {
      val configBuilder = SimpleTypesafeConfigBuilder
      val conf = configBuilder.loadConfiguration(Array("MockApp.param=\"param\""), "fun.conf").get
      conf.getString("MockApp.param") shouldBe "param"
      conf.getString("MockApp.whoami") shouldBe "./src/test/resources/reference.conf"
      conf.getString("MockApp.reference") shouldBe "reference_mock_app"
      Try(conf.getStringList("MockApp.some.list")).toOption shouldBe None
      Try(conf.getBoolean("MockApp.file.application.conf")).toOption shouldBe None
    }
    "perform variable substitution" in {
      val configBuilder = SimpleTypesafeConfigBuilder
      val conf = configBuilder.loadConfiguration(Array(
        "MockFun.param=\"param\"", "my.var=\"MYVAR\""), "fun.conf").get
      conf.getString("MockFun.param") shouldBe "param"
      conf.getString("MockFun.whoami") shouldBe "./src/test/resources/reference.conf"
      conf.getString("MockFun.reference") shouldBe "reference_mock_fun"
      Try(conf.getString("MockFun.substitute.my-var")).toOption shouldBe None
      Try(conf.getString("MockFun.substitute.my-other-var")).toOption shouldBe None
      Try(conf.getStringList("MockFun.some.list")).toOption shouldBe None
      Try(conf.getBoolean("MockFun.file.application.conf")).toOption shouldBe None
    }
  }

}
