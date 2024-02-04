package org.tupol.spark.io.streaming.structured

import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.io.FormatType

class StreamingSinkConfigurationSpec extends AnyWordSpec with Matchers {

  val testSchema = StructType(Seq(StructField("test-col", StringType, false)))

  "addOptions" should {

    val options         = Map("prop1" -> "val1", "prop2"         -> "val2")
    val extraOptions    = Map("prop2" -> "VAL2-CHANGED", "prop3" -> "val3")
    val expectedOptions = Map("prop1" -> "val1", "prop2"         -> "VAL2-CHANGED", "prop3" -> "val3")

    "overwrite existing options for GenericStreamDataSinkConfiguration" in {
      val tested          = GenericStreamDataSinkConfiguration(FormatType.Custom(""), options = options)
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions.get should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for FileStreamDataSinkConfiguration" in {
      val tested = FileStreamDataSinkConfiguration(
        path = "path",
        genericConfig = GenericStreamDataSinkConfiguration(FormatType.Custom(""), options, None),
        checkpointLocation = None
      )
      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions should contain allElementsOf expectedOptions
    }

    "overwrite existing options for KafkaStreamDataSinkConfiguration" in {
      val tested = KafkaStreamDataSinkConfiguration(
        kafkaBootstrapServers = "kafkaBootstrapServers",
        genericConfig = GenericStreamDataSinkConfiguration(FormatType.Custom(""), options, None),
        options = options
      )
      val result          = tested.addOptions(extraOptions)
      val resultedOptions = result.options
      resultedOptions should contain allElementsOf expectedOptions
      result.generic.options.get should contain allElementsOf expectedOptions
    }
  }

}
