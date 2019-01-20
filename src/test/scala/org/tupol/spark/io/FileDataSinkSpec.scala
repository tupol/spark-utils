package org.tupol.spark.io

import org.apache.spark.sql.DataFrame
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.{ Failure, Success }

class FileDataSinkSpec extends FunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  test("Saving the input data results in the same data") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)

    val sinkConfig = FileSinkConfiguration(testPath1, FormatType.Parquet)
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    val writtenData: DataFrame = spark.read.parquet(testPath1)
    writtenData.comapreWith(inputData).areEqual(true) shouldBe true
  }

  test("Saving the input data can fail if the mode is default and the target file already exists") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)

    val sinkConfig = FileSinkConfiguration(testPath1, FormatType.Parquet)
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    inputData.sink(sinkConfig).write shouldBe a[Failure[_]]
    a[DataSinkException] should be thrownBy (inputData.sink(sinkConfig).write.get)
  }

  test("Saving the input partitioned results in the same data") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val inputData: DataFrame = spark.read.parquet(inputPath)
    val rootPartition = "int"
    val childPartition = "string"

    val sinkConfig = FileSinkConfiguration(testPath1, FormatType.Parquet, None, None, Seq(rootPartition, "string"))
    inputData.sink(sinkConfig).write shouldBe a[Success[_]]

    val writtenData: DataFrame = spark.read.parquet(testPath1)
    writtenData.comapreWith(inputData).areEqual(true) shouldBe true

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
    val partitionFiles = 1

    val sinkConfig = FileSinkConfiguration(testPath1, FormatType.Parquet, None, Some(partitionFiles), Seq(rootPartition, "string"))
    inputData.sink(sinkConfig).write

    val writtenData: DataFrame = spark.read.parquet(testPath1)
    writtenData.comapreWith(inputData).areEqual(true) shouldBe true

    val intPartitions = testFile1.listFiles().filter(_.getPath.contains(s"/$rootPartition="))
    intPartitions.size should be > 0

    val stringPartitions = intPartitions.flatMap(_.listFiles().filter(_.getPath.contains(s"/$childPartition=")))
    stringPartitions.size should be >= intPartitions.size

    val filesPerChildPartition = stringPartitions.map(_.list().filterNot(_.endsWith(".crc")).size)
    filesPerChildPartition should contain theSameElementsAs (stringPartitions.map(_ => partitionFiles))
  }

}
