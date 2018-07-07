package org.tupol.spark.config

import com.typesafe.config.ConfigException.BadValue
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalatest.Matchers
import Extractor.rangeExtractor.parseStringToRange

class StringToRangeProp extends Properties("Range") with Matchers {

  property("commons#parseStringToRange - creates sequence for a single int value") = forAll { (i: Int) =>
    val input = i.toString
    val output = parseStringToRange(input, "somePath")

    output.head == i
  }

  property("commons#parseStringToRange - throws BadValue for any string") = forAll { (input: String) =>
    val ex = intercept[BadValue] {
      parseStringToRange(input, "somePath")
    }
    ex.isInstanceOf[BadValue]
  }

  property("commons#parseStringToRange - throws BadValue for any string separated by commas") = forAll { (start: String, stop: String, step: String) =>
    val input = start + "," + stop + "," + step
    val ex = intercept[BadValue] {
      parseStringToRange(input, "somePath")
    }
    ex.isInstanceOf[BadValue]
  }
}
