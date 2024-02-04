package org.tupol.spark.io

import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.io.sources._

class SourceConfigurationTest extends AnyWordSpec with Matchers {

  val testSchema = StructType(Seq(StructField("test-col", StringType, false)))

  "schemaWithCorruptRecord" should {
    "return None if there is no schema defined" in {
      val sourceConfig = new SourceConfiguration {
        override def format: FormatType                                                 = ???
        override def options: Map[String, String]                                       = ???
        override def schema: Option[StructType]                                         = None
        override def addOptions(extraOptions: Map[String, String]): SourceConfiguration = ???
        override def withSchema(schema: Option[StructType]): SourceConfiguration        = ???
      }
      sourceConfig.schemaWithCorruptRecord shouldBe None
    }
    "return schema if columnNameOfCorruptRecord is not present in options" in {
      val sourceConfig = new SourceConfiguration {
        override def format: FormatType                                                 = ???
        override def options: Map[String, String]                                       = Map()
        override def schema: Option[StructType]                                         = Some(testSchema)
        override def addOptions(extraOptions: Map[String, String]): SourceConfiguration = ???
        override def withSchema(schema: Option[StructType]): SourceConfiguration        = ???
      }
      sourceConfig.schemaWithCorruptRecord shouldBe Some(testSchema)
    }
    "return schema with columnNameOfCorruptRecord extra column" in {
      val sourceConfig = new SourceConfiguration {
        override def format: FormatType                                                 = ???
        override def options: Map[String, String]                                       = Map(ColumnNameOfCorruptRecord -> "error")
        override def schema: Option[StructType]                                         = Some(testSchema)
        override def addOptions(extraOptions: Map[String, String]): SourceConfiguration = ???
        override def withSchema(schema: Option[StructType]): SourceConfiguration        = ???
      }
      val expectedSchema =
        StructType(Seq(StructField("test-col", StringType, false), StructField("error", StringType, true)))
      sourceConfig.schemaWithCorruptRecord shouldBe Some(expectedSchema)
    }
  }

  "addOptions" should {

    val options         = Map("prop1" -> "val1", "prop2"         -> "val2")
    val extraOptions    = Map("prop2" -> "VAL2-CHANGED", "prop3" -> "val3")
    val expectedOptions = Map("prop1" -> "val1", "prop2"         -> "VAL2-CHANGED", "prop3" -> "val3")

    "overwrite existing options for CsvSourceConfiguration" in {
      val tested          = CsvSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for XmlSourceConfiguration" in {
      val tested          = XmlSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for JsonSourceConfiguration" in {
      val tested          = JsonSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for ParquetSourceConfiguration" in {
      val tested          = ParquetSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for OrcSourceConfiguration" in {
      val tested          = OrcSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for AvroSourceConfiguration" in {
      val tested          = AvroSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for TextSourceConfiguration" in {
      val tested          = TextSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for JdbcSourceConfiguration" in {
      val tested          = JdbcSourceConfiguration(options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for GenericSourceConfiguration" in {
      val tested          = GenericSourceConfiguration(FormatType.Custom(""), options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

  }

}
