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

import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.tupol.utils.implicits._

import scala.util.Try

/**
 * Trivial trait for executing basic Spark runnable applications.
 *
 * @tparam Context the type of the application context class.
 * @tparam Result The output type of the run function.
 *
 */
trait SparkApp[Context, Result] extends SparkRunnable[Context, Result] with TypesafeConfigBuilder with Logging {

  /**
   * This is the key for basically choosing a certain app and it should have
   * the form of '_APP_NAME_....', reflected also in the configuration structure.
   *
   * By default this will return the simple class name.
   */
  def appName: String = getClass.getSimpleName.replaceAll("\\$", "")

  /**
   * This function needs to be implemented and should contain all logic related
   * to parsing the configuration settings and building the application context.
   */
  def createContext(config: Config): Try[Context]

  /**
   * Any object extending this trait becomes a runnable application.
   *
   * @param args
   */
  def main(implicit args: Array[String]): Unit = {
    log.info(s"Running $appName")
    implicit val spark = createSparkSession(appName)
    implicit val conf = applicationConfiguration

    val outcome = for {
      context <- createContext(conf)
      result <- run(spark, context)
    } yield result

    outcome
      .logSuccess(_ => log.info(s"$appName: Job successfully completed."))
      .logFailure(t => log.error(s"$appName: Job failed.", t))

    // Close the session so the application can exit
    Try(spark.close)
      .logSuccess(_ => log.info(s"$appName: Spark session closed."))
      .logFailure(t => log.error(s"$appName: Failed to close the spark session.", t))

    // If the application failed we exit with an exception
    outcome.get
  }

  protected def createSparkSession(runnerName: String) = {
    val defSparkConf = new SparkConf(true)
    val sparkConf = defSparkConf.setAppName(runnerName).
      setMaster(defSparkConf.get("spark.master", "local[*]"))
    SparkSession.builder.config(sparkConf).getOrCreate()
  }
}
