package org.tupol.spark.io

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.tupol.spark.io.sources._

class SinkConfigurationTest extends AnyWordSpec with Matchers {

  "addOptions" should {

    val options = Map("prop1" -> "val1", "prop2" -> "val2")
    val extraOptions = Map("prop2" -> "VAL2-CHANGED", "prop3" -> "val3")
    val expectedOptions = Map("prop1" -> "val1", "prop2" -> "VAL2-CHANGED", "prop3" -> "val3")

    "overwrite existing options for FileSinkConfiguration" in {

      val tested = FileSinkConfiguration(
        path = "path",
        format = FormatType.Custom(""),
        mode = None,
        partition = None,
        buckets = None,
        options = Some(options)
      )

      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions.get should contain theSameElementsAs expectedOptions
    }

    "overwrite existing options for GenericSinkConfiguration" in {

      val tested = GenericSinkConfiguration(
        format = FormatType.Custom(""),
        mode = None,
        partition = None,
        buckets = None,
        options = Some(options)
      )

      val resultedOptions = tested.addOptions(extraOptions).options
      resultedOptions.get should contain theSameElementsAs expectedOptions
    }


  }

}
