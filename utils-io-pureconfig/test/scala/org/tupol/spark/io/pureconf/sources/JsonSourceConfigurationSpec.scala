package org.tupol.spark.io.pureconf.sources

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.io.pureconf.SourceConfigurator
import org.tupol.spark.io.sources.JsonSourceConfiguration
import org.tupol.spark.sql.loadSchemaFromFile

import scala.util.Success

class JsonSourceConfigurationSpec extends AnyWordSpec with Matchers {

  val ReferenceSchema = loadSchemaFromFile("sources/avro/sample_schema.json").get

 "Parse configuration" should {

   "work with schema and no options" in {

     val expected = JsonSourceConfiguration(Map(), Some(ReferenceSchema))

     val configStr =
       s"""
         |format="json"
         |path="INPUT_PATH"
         |schema: ${ReferenceSchema.prettyJson}
         |""".stripMargin

     val config = ConfigFactory.parseString(configStr)
     val result = SourceConfigurator.extract(config)

     result shouldBe Success(expected)

   }

   "work without schema" in {

     val expected = JsonSourceConfiguration()

     val configStr =
       """
         |format="json"
         |path="INPUT_PATH"
         |""".stripMargin

     val config = ConfigFactory.parseString(configStr)
     val result = SourceConfigurator.extract(config)

     result shouldBe Success(expected)

   }

   "work with options" in {

     val expected = JsonSourceConfiguration(
       Map("mode" -> "PERMISSIVE",
       "columnNameOfCorruptRecord" -> "_arbitrary_name"))

     val configStr =
       """
         |format="json"
         |path="INPUT_PATH"
         |options={
         |   mode : "PERMISSIVE"
         |   columnNameOfCorruptRecord : "_arbitrary_name"
         |}
                    """.stripMargin

     val config = ConfigFactory.parseString(configStr)
     val result = SourceConfigurator.extract(config)

     result shouldBe Success(expected)
   }

   "fail when path is missing" in {
     val configStr =
       """
         |format="json"
         |mode="FAILFAST"
                    """.stripMargin

     val config = ConfigFactory.parseString(configStr)
     val result = SourceConfigurator.extract(config)

     result shouldBe a[Success[_]]
     result.get shouldBe a[JsonSourceConfiguration]
     result.get.schema.isDefined shouldBe false

   }
 }



}
