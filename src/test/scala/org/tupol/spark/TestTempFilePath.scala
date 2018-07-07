package org.tupol.spark

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.util.Try

/**
 * Simple trait that generates a temporary test file path before each test and removes it after
 */
trait TestTempFilePath extends BeforeAndAfterEach {
  this: Suite =>

  var _testFile: String = _

  def testFile = _testFile

  override def beforeEach(): Unit = {
    super.beforeEach()
    _testFile = s"/tmp/temp_${UUID.randomUUID().toString}"
  }

  override def afterEach(): Unit = {
    Try(FileUtils.forceDelete(new File(_testFile)))
    super.afterEach()
  }

}
