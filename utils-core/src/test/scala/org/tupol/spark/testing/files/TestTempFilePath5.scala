package org.tupol.spark.testing.files

import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfterEach, Suite }

import scala.util.Try

/** Simple trait that generates a temporary test file path before each test and removes it after */
trait TestTempFilePath5 extends BeforeAndAfterEach {
  this: Suite =>

  private val tempDir = Option(System.getProperty("java.io.tmpdir")).getOrElse("/tmp")
  private var _tempFile: java.io.File = _

  def testFile5 = _tempFile
  def testPath5 = _tempFile.getAbsolutePath

  override def beforeEach(): Unit = {
    super.beforeEach()
    _tempFile = new java.io.File(s"$tempDir/spark_tests_${UUID.randomUUID().toString}.temp")
  }

  override def afterEach(): Unit = {
    Try(FileUtils.forceDelete(_tempFile))
    super.afterEach()
  }

}
