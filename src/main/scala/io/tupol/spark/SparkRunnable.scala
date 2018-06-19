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
package io.tupol.spark

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkFiles }

import scala.util.{ Failure, Success, Try }

/**
 * Trivial trait for running basic Spark applications.
 * @tparam Configuration the type of the application configuration class.
 * @tparam Result The output type of the run method.
 *
 */
trait SparkRunnable[Configuration, Result] extends Logging {

  /**
   * This is the key for basically choosing a certain app and it should have
   * the form of '_APP_NAME_....', reflected also in the configuration structure.
   *
   * @return
   */
  def appName: String = getClass.getSimpleName.replaceAll("\\$", "")

  /**
   * This method needs to be implemented and should contain all logic related
   * to parsing the configuration settings.
   */
  def buildConfig(config: Config): Try[Configuration]

  val skippedPathsWhenLogging: Seq[String] = Nil

  /**
   * This method needs to be implemented and should contain the entire runnable logic.
   *
   * @param spark active spark session
   * @param config configuration class
   * @return
   */
  def run(spark: SparkSession, config: Configuration): Try[Result]

  /**
   * Any object extending this trait becomes a runnable application.
   *
   * @param args
   */
  def main(implicit args: Array[String]): Unit = {
    val runnableName = this.getClass.getName
    log.info(s"Running $runnableName")
    val spark = createSparkSession(runnableName)
    val conf = applicationConfiguration(spark)

    val outcome = for {
      config <- buildConfig(conf)
      result <- run(spark, config)
    } yield result

    outcome match {
      case _: Success[_] =>
        log.info(s"$appName: Job successfully completed.")
      case Failure(ex) =>
        log.error(s"$appName: Job failed.", ex)
    }
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
  private[spark] def applicationConfiguration(spark: SparkSession)(implicit args: Array[String]) = {

    import java.io.File

    val CONFIGURATION_FILENAME = "application.conf"

    log.info(s"$appName: Application Parameters:\n${args.mkString("\n")}")

    // This configuration file is supposed to work with the --files option of spark-submit, but it seems that in yarn-cluster mode this one fails.
    // In yarn cluster mode the SparkFiles.getRootDirectory yields a result like
    //   /opt/app/hadoop/yarn/local/usercache/spark/appcache/application_1472457872363_0064/spark-fb5e850b-2108-482d-8dff-f9a3d2db8dd6/userFiles-d02bb426-515d-4e82-a2ac-d160061c8cb6/
    // However, the local path (new File(".").getAbsolutePath() ) for the Driver looks like
    //   /opt/app/hadoop/yarn/local/usercache/spark/appcache/application_1472457872363_0064/container_e21_1472457872363_0064_01_000001
    // Though looks like a small difference, this is a problem in cluster mode.
    // To overcome situations encountered so far we are using the `configurationFile` and the `localConfigurationFile` to try both paths.
    // In standalone mode the reverse is true.
    // We might be able to come to the bottom of this, but it looks like a rabbit hole not worth exploring at the moment.
    val configurationFile: Option[File] = {
      val file = new File(SparkFiles.get(CONFIGURATION_FILENAME))
      val available = file.exists && file.canRead && file.isFile
      log.info(s"$appName: SparkFiles configuration file: ${file.getAbsolutePath} is ${if (!available) "not " else ""}available.")
      if (available) Some(file) else None
    }

    val localConfigurationFile: Option[File] = {
      val file = new File(CONFIGURATION_FILENAME)
      val available = file.exists && file.canRead && file.isFile
      log.info(s"$appName: Local configuration file: ${file.getAbsolutePath} is ${if (!available) "not " else ""}available.")
      if (available) Some(file) else None
    }

    val configurationFiles = Seq(configurationFile, localConfigurationFile)

    val parametersConf = ConfigFactory.parseString(args.mkString("\n"))
    val fullConfig =
      configurationFiles.filter(_.isDefined).map(_.get).
        foldLeft(parametersConf)((cfg, file) => cfg.withFallback(ConfigFactory.parseFile(file))).
        withFallback(ConfigFactory.parseResources(s"/$CONFIGURATION_FILENAME")).
        withFallback(ConfigFactory.defaultReference())
    val config = fullConfig.getConfig(appName)

    log.debug(s"$appName: Configuration:\n${renderConfig(config)}")

    config
  }

  protected def renderConfig(config: Config): String = skippedPathsWhenLogging.foldLeft(config)(_.withoutPath(_)).root.render
}
