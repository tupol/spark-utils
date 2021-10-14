package org.tupol.spark.io

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.Success

class GenericDataSinkSpec extends AnyFunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  val CustomFormat = FormatType.Custom("com.databricks.spark.avro")

  override val sparkConfig = super.sparkConfig + ("spark.io.compression.codec" -> "snappy")

  test("Saving the input data results in the same data") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)

    val options = Map[String, String]("path" -> testPath1)
    val sinkConfig = GenericSinkConfiguration(CustomFormat, None, Seq(), None, options)
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    val writtenData: DataFrame = spark.read.format(CustomFormat.format).load(testPath1)
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

  test("Saving the input data can fail if the mode is default and the target file already exists") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)

    val options = Map[String, String]("path" -> testPath1)
    val sinkConfig = GenericSinkConfiguration(CustomFormat, None, Seq(), None, options)
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    inputData.sink(sinkConfig).write.failed.get shouldBe a[DataSinkException]
  }

  test("Saving the input partitioned results in the same data") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)
    val rootPartition = "int"
    val childPartition = "string"

    val options = Map[String, String]("path" -> testPath1)
    val sinkConfig = GenericSinkConfiguration(CustomFormat, None, Seq(rootPartition, "string"), None, options)
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    val writtenData: DataFrame = spark.read.format(CustomFormat.format).load(testPath1)
    writtenData.compareWith(inputData).areEqual(true) shouldBe true

    val intPartitions = testFile1.listFiles().filter(_.getPath.contains(s"/$rootPartition="))
    intPartitions.size should be > 0

    val stringPartitions = intPartitions.flatMap(_.listFiles().filter(_.getPath.contains(s"/$childPartition=")))
    stringPartitions.size should be >= intPartitions.size
  }

  test("Saving the input partitioned with a partition number specified results in the same data") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)
    val rootPartition = "int"
    val childPartition = "string"

    val options = Map[String, String]("path" -> testPath1)
    val sinkConfig = GenericSinkConfiguration(CustomFormat, None, Seq(rootPartition, "string"), None, options)
    inputData.sink(sinkConfig).write

    val writtenData: DataFrame = spark.read.format(CustomFormat.format).load(testPath1)
    writtenData.compareWith(inputData).areEqual(true) shouldBe true

    val intPartitions = testFile1.listFiles().filter(_.getPath.contains(s"/$rootPartition="))
    intPartitions.size should be > 0

    val stringPartitions = intPartitions.flatMap(_.listFiles().filter(_.getPath.contains(s"/$childPartition=")))
    stringPartitions.size should be >= intPartitions.size
  }

  test("Saving the input bucketed results in the same data") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)
    val buckets = BucketsConfiguration(1, Seq("int", "string"), Seq("int", "string"))

    val options = Map[String, String]("path" -> testPath1)
    val sinkConfig = GenericSinkConfiguration(CustomFormat, None, Seq(), Some(buckets), options)
    inputData.sink(sinkConfig).write

    val writtenData: DataFrame = spark.read.format(CustomFormat.format).load(testPath1)
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

}
