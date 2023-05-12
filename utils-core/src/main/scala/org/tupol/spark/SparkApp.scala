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
import org.apache.spark.sql.SparkSession
import org.tupol.spark.config.{FuzzyTypesafeConfigBuilder, TypesafeConfigBuilder, renderConfig}
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
  def appName: String = simpleClassName(this)

  /** The configuration file that the application will look for in order to resolve the configuration.
   * This will be further used by the `getApplicationConfiguration` method */
  def configurationFileName = "application.conf"

  /**
   * This function needs to be implemented and should contain all logic related
   * to parsing the configuration settings and building the application context.
   */
  def createContext(config: Config): Try[Context]

  /** Any object extending this trait becomes a runnable application. */
  def main(implicit args: Array[String]): Unit = {
    log.info(s"Running $appName")
    val outcome = for {
      config <- loadConfiguration(args, configurationFileName)
      appConfig <- getApplicationConfiguration(config)
      context <- createContext(appConfig)
      spark    = createSparkSession(appName)
      result  <- run(spark, context)
    } yield result

    outcome
      .logSuccess(_ => log.info(s"$appName: Job successfully completed."))
      .logFailure(t => log.error(s"$appName: Job failed.", t))

    // If the application failed we exit with an exception
    outcome.get
  }

  override def loadConfiguration(args: Seq[String], configurationFileName: String): Try[Config] = {
    val fuzzyArgs = (args ++ args.map(arg => s"$appName.$arg"))
    FuzzyTypesafeConfigBuilder.loadConfiguration(fuzzyArgs, configurationFileName)
  }

  protected def createSparkSession(appName: String) =
    SparkSession.builder.appName(appName).getOrCreate()


  /**
   * Extract and assemble a configuration object out of the global configuration.
   * It will try to extract the configuration from the application name; if tha fails it will default back to the
   * root configuration, on the off-chance that the application configuration was not structured inside a named root.
   *
   * @param config global configuration object, that should contain also a root configuration with the same name as the `appName`
   * @return the application configuration object
   */
  def getApplicationConfiguration(config: Config): Try[Config] =
    Try(config.getConfig(appName))
      .logSuccess(config => log.debug(s"$appName: Configuration:\n${renderConfig(config)}"))
      .recover { case t =>
        log.error(s"$appName: Failed to load application configuration from the $appName path; using the root configuration instead; ${t.getMessage}")
        config
      }

}
