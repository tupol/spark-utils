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

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkFiles }

import scala.util.Try
import org.tupol.utils._

/**
 * Trivial trait for executing basic Spark runnable applications.
 *
 * @tparam Context the type of the application context class.
 * @tparam Result The output type of the run function.
 *
 */
trait SparkApp[Context, Result] extends SparkRunnable[Context, Result] with Logging {

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
  def createContext(config: Config): Context

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
      context <- Try(createContext(conf))
      result <- Try(run(spark, context))
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

  /**
   * Extract and assemble a configuration object out of the application parameters and
   * configuration files, as described bellow.
   * <p>
   * The parameters assembly priority order is as following (the top ones are overwriting the bottom ones, if defined):
   * <ol>
   * <li> application parameters passed in as `main()` arguments</li>
   * <li> `application.conf` file, if passed in as a `spark-submit --files ...` argument</li>
   * <li> `application.conf` file, if available in the classpath</li>
   * <li> `reference.conf` file, if available in the classpath</li>
   * </ol>
   * @param spark the spark session
   * @param args application parameters
   * @return the application configuration object
   */
  private[spark] def applicationConfiguration(implicit spark: SparkSession, args: Array[String]) = {

    import java.io.File

    val CONFIGURATION_FILENAME = "application.conf"

    log.info(s"$appName: Application Parameters:\n${args.mkString("\n")}")

    // This configuration file is supposed to work with the --files option of spark-submit,
    // but it seems that in yarn-cluster mode this one fails.
    // In yarn cluster mode the SparkFiles.getRootDirectory yields a result like
    //   /opt/app/hadoop/yarn/local/usercache/spark/appcache/application_1472457872363_0064/spark-fb5e850b-2108-482d-8dff-f9a3d2db8dd6/userFiles-d02bb426-515d-4e82-a2ac-d160061c8cb6/
    // However, the local path (new File(".").getAbsolutePath() ) for the Driver looks like
    //   /opt/app/hadoop/yarn/local/usercache/spark/appcache/application_1472457872363_0064/container_e21_1472457872363_0064_01_000001
    // Though looks like a small difference, this is a problem in cluster mode.
    // To overcome situations encountered so far we are using the `configurationFile` and the `localConfigurationFile`
    // to try both paths.
    // In standalone mode the reverse is true.
    // We might be able to come to the bottom of this, but it looks like a rabbit hole not worth exploring at the moment.
    val sparkConfiguration: Option[Config] = {
      val file = new File(SparkFiles.get(CONFIGURATION_FILENAME))
      val available = file.exists && file.canRead && file.isFile
      log.info(s"$appName: SparkFiles configuration file: ${file.getAbsolutePath} " +
        s"is ${if (!available) "not " else ""}available.")
      if (available) {
        Try(ConfigFactory.parseFile(file))
          .logSuccess(_ => log.info(s"Successfully parsed the local file at '${file.getAbsolutePath}'"))
          .logFailure(t => log.error(s"Failed to parse local file at '${file.getAbsolutePath}'", t))
          .toOption
      } else None
    }

    val localConfiguration: Option[Config] = {
      val file = new File(CONFIGURATION_FILENAME)
      val available = file.exists && file.canRead && file.isFile
      log.info(s"$appName: Local configuration file: ${file.getAbsolutePath} is ${if (!available) "not " else ""}available.")
      if (available) {
        Try(ConfigFactory.parseFile(file))
          .logSuccess(_ => log.info(s"Successfully parsed the local file at '${file.getAbsolutePath}'"))
          .logFailure(t => log.error(s"Failed to parse local file at '${file.getAbsolutePath}'", t))
          .toOption
      } else None
    }

    val classpathConfiguration: Option[Config] = {
      val resourcePath = s"/$CONFIGURATION_FILENAME"
      Try(ConfigFactory.parseResources(resourcePath))
        .logSuccess(_ => log.info(s"Successfully parsed the classpath configuration at '$resourcePath'"))
        .logFailure(t => log.error(s"Failed to parse classpath configuration at '$resourcePath'", t))
        .toOption
    }

    val configurationFiles = Seq(sparkConfiguration, localConfiguration, classpathConfiguration)

    val parametersConf = ConfigFactory.parseString(args.mkString("\n"))
    val fullConfig =
      configurationFiles.collect { case Some(config) => config }.
        foldLeft(parametersConf)((acc, conf) => acc.withFallback(conf)).
        withFallback(ConfigFactory.defaultReference())
    val resolvedConfig = Try(fullConfig.resolve())
      .logFailure(_ => log.warn("Failed to resolve the variables locally."))
      .orElse(Try(fullConfig.resolveWith(parametersConf)))
      .logFailure(_ => log.warn("Failed to resolve the variables from the application arguments."))
      .getOrElse(fullConfig)
    val config = Try(resolvedConfig.getConfig(appName)).getOrElse(ConfigFactory.empty())

    log.debug(s"$appName: Configuration:\n${renderConfig(config)}")

    config
  }

  protected def renderConfig(config: Config): String = config.root.render
}
