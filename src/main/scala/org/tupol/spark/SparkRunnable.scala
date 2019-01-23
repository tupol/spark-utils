/*
MIT License

Copyright (c) 2018 Tupol (github.com/tupol)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package org.tupol.spark

import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * Trivial trait for creating basic runnable Spark applications.
 * These runnable still needs a runner or an app to run.
 *
 * @tparam Configuration the type of the application configuration class.
 * @tparam Result The output type of the run method.
 *
 */
trait SparkRunnable[Configuration, Result] {

  /**
   * This method needs to be implemented and should contain the entire runnable logic.
   *
   * @param config configuration instance that should contain all the application specific configuration
   * @param spark active spark session
   * @return
   */
  def run(implicit spark: SparkSession, config: Configuration): Try[Result]

}
