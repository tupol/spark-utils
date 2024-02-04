package org.tupol.spark.io.streaming.structured

import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.sources._

class StreamingSourceConfigurationTest extends AnyWordSpec with Matchers {

  val testSchema = StructType(Seq(StructField("test-col", StringType, false)))

  "addOptions" should {

    val options         = Map("prop1" -> "val1", "prop2"         -> "val2")
    val extraOptions    = Map("prop2" -> "VAL2-CHANGED", "prop3" -> "val3")
    val expectedOptions = Map("prop1" -> "val1", "prop2"         -> "VAL2-CHANGED", "prop3" -> "val3")

    "overwrite existing options for FileStreamDataSourceConfiguration" in {
      val tested = FileStreamDataSourceConfiguration(
        path = "path",
        sourceConfiguration = GenericSourceConfiguration(FormatType.Custom(""), options, None)
      )
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for GenericStreamDataSourceConfiguration" in {
      val tested          = GenericStreamDataSourceConfiguration(FormatType.Custom(""), options, None)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for KafkaStreamDataSourceConfiguration" in {
      val tested = KafkaStreamDataSourceConfiguration(
        kafkaBootstrapServers = "",
        subscription = KafkaSubscription("", ""),
        options = options
      )
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain theSameElementsAs expectedOptions
    }

  }

}
