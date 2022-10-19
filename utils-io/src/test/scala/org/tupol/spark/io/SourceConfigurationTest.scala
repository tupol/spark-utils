package org.tupol.spark.io

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.io.sources.{ColumnNameOfCorruptRecord, SourceConfiguration}

class SourceConfigurationTest extends AnyWordSpec with Matchers {

  val testSchema = StructType(Seq(StructField("test-col", StringType, false)))

  "schemaWithCorruptRecord" should {
    "return None if there is no schema defined" in {
      val sourceConfig = new SourceConfiguration{
        override def format: FormatType = ???
        override def options: Map[String, String] = ???
        override def schema: Option[StructType] = None
      }
      sourceConfig.schemaWithCorruptRecord shouldBe None
    }
    "return schema if columnNameOfCorruptRecord is not present in options" in {
      val sourceConfig = new SourceConfiguration{
        override def format: FormatType = ???
        override def options: Map[String, String] = Map()
        override def schema: Option[StructType] = Some(testSchema)
      }
      sourceConfig.schemaWithCorruptRecord shouldBe Some(testSchema)
    }
    "return schema with columnNameOfCorruptRecord extra column" in {
      val sourceConfig = new SourceConfiguration{
        override def format: FormatType = ???
        override def options: Map[String, String] = Map(ColumnNameOfCorruptRecord -> "error")
        override def schema: Option[StructType] = Some(testSchema)
      }
      val expectedSchema = StructType(Seq(StructField("test-col", StringType, false), StructField("error", StringType, true)))
      sourceConfig.schemaWithCorruptRecord shouldBe Some(expectedSchema)
    }
  }

}
