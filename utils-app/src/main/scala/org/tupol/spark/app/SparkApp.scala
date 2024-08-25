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
package org.tupol.spark.app

import com.typesafe.config.Config
import org.tupol.spark.Logging
import org.tupol.spark.app.config._
import org.tupol.utils.TryOps.TryOps

import scala.util.Try


/**
 * Trivial trait for executing basic Spark runnable applications.
 *
 * @tparam Context the type of the application context class.
 * @tparam Result The output type of the run function.
 *
 */
trait SparkApp[Context, Result] extends org.tupol.spark.SparkApp[Context, Result] with TypesafeConfigBuilder with Logging {


  /** The configuration file that the application will look for in order to resolve the configuration.
   * This will be further used by the `getApplicationConfiguration` method */
  def configurationFileName = "application.conf"

  /** Convert a typesafe config into an application context*/
  def createContext(config: Config): Try[Context]

  override def loadConfiguration(args: Seq[String], configurationFileName: String): Try[Config] = {
    val fuzzyArgs = (args ++ args.map(arg => s"$appName.$arg"))
    FuzzyTypesafeConfigBuilder.loadConfiguration(fuzzyArgs, configurationFileName)
  }

  override def createContext(args: Array[String]): Try[Context] =
    for {
      loadedConf <- loadConfiguration(args, configurationFileName)
      appConf <- getApplicationConfiguration(loadedConf)
      context <- createContext(appConf)
    } yield context

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
      .recover {
        case t =>
          log.error(
            s"$appName: Failed to load application configuration from the $appName path; using the root configuration instead; ${t.getMessage}"
          )
          config
      }

}
