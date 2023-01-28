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
package org.tupol

import org.tupol.utils.implicits._
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.SparkFiles
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{ Encoder, SparkSession }

import scala.util.Try

package object spark {

  /**
   * `TypesafeConfigBuilder` must be mixed in with a class or a trait that has an `appName` function
   * and it provides the `applicationConfiguration` function that builds a Typesafe `Config` instance
   * out of various resources including application parameters.
   */
  trait TypesafeConfigBuilder extends Logging {
    this: { def appName: String } =>

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
    def getApplicationConfiguration(args: Array[String])(implicit spark: SparkSession): Config = {

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

  object encoders {

    /**
     * This code is taken from [[org.apache.spark.sql.catalyst.encoders]] to work around a Databricks runtime issue.
     *
     * Returns an internal encoder object that can be used to serialize / deserialize JVM objects
     * into Spark SQL rows.  The implicit encoder should always be unresolved (i.e. have no attribute
     * references from a specific schema.)  This requirement allows us to preserve whether a given
     * object type is being bound by name or by ordinal when doing resolution.
     */
    def encoderFor[A: Encoder]: ExpressionEncoder[A] =
      implicitly[Encoder[A]] match {
        case e: ExpressionEncoder[A] =>
          e.assertUnresolved()
          e
        case _ => sys.error(s"Only expression encoders are supported today")
      }

    def tuple2[A: Encoder, B: Encoder]: Encoder[(A, B)] =
      org.apache.spark.sql.Encoders.tuple(encoderFor[A], encoderFor[B])
  }
}
