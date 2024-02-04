package org.tupol.spark

import org.tupol.utils.implicits._
import com.typesafe.config.{ Config, ConfigFactory, ConfigParseOptions, ConfigRenderOptions, ConfigResolveOptions }
import org.apache.spark.SparkFiles

import scala.io.Source
import scala.util.Try

object config {

  def renderConfig(config: Config): String =
    config.root.render(ConfigRenderOptions.defaults().setComments(false).setOriginComments(false))

  /**
   * `TypesafeConfigBuilder` must be mixed in with a class or a trait that has an `appName` function
   * and it provides the `applicationConfiguration` function that builds a Typesafe `Config` instance
   * out of various resources including application parameters.
   */
  trait TypesafeConfigBuilder extends Logging {

    /**
     * Extract and assemble a root configuration object out of the application parameters and
     * configuration files.
     *
     * @param args application parameters
     * @param configurationFileName load main configuration from this configuration file
     * @return the application configuration object
     */
    def loadConfiguration(
      args: Seq[String],
      configurationFileName: String = "application.conf"
    ): Try[Config]

  }

  object SimpleTypesafeConfigBuilder extends TypesafeConfigBuilder {
    def loadConfiguration(
      args: Seq[String],
      configurationFileName: String = "application.conf"
    ): Try[Config] =
      for {
        argsConfig     <- Try(ConfigFactory.parseString(args.mkString("\n")))
        resolvedConfig <- resolveConfig(argsConfig, configurationFileName)
      } yield resolvedConfig

    private def resolveConfig(inputConfig: Config, fallbackResource: String): Try[Config] =
      Try {
        val defaultConfig = ConfigFactory
          .load(
            ConfigParseOptions.defaults,
            ConfigResolveOptions.defaults.setAllowUnresolved(true)
          )
        val unresolvedConfig = ConfigFactory
          .load(
            fallbackResource,
            ConfigParseOptions.defaults,
            ConfigResolveOptions.defaults.setAllowUnresolved(true)
          )
        val resolvedConfig = inputConfig
          .withFallback(unresolvedConfig)
          .withFallback(defaultConfig)
          .resolve()
        resolvedConfig
      }
  }

  object FuzzyTypesafeConfigBuilder extends TypesafeConfigBuilder {

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
     *
     * @param spark the spark session
     * @param args  application parameters
     * @param configurationFileName load main configuration from this configuration file
     * @return the application configuration object
     */
    def loadConfiguration(
      args: Seq[String],
      configurationFileName: String = "application.conf"
    ): Try[Config] = Try {

      import java.io.File

      log.info(s"Arguments:\n${args.mkString("\n")}")

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

      def configFromPath(path: String, label: Option[String] = None): Option[Config] = {
        val file      = new File(path)
        val available = file.exists && file.canRead && file.isFile
        val lab       = s"${label.getOrElse("Local file")}"
        log.info(
          s"$lab: ${file.getAbsolutePath} is ${if (!available) "not " else ""}available."
        )
        if (available) {
          Try(ConfigFactory.parseFile(file))
            .logSuccess(_ => log.info(s"$lab: successfully parsed at '${file.getAbsolutePath}'"))
            .logFailure(t => log.error(s"$lab: failed to parse at '${file.getAbsolutePath}'", t))
            .toOption
        } else None
      }

      val sparkConfiguration: Option[Config] =
        for {
          path   <- Try(SparkFiles.get(configurationFileName)).toOption
          result <- configFromPath(path, Some("SparkFiles configuration file"))
        } yield result

      val localConfiguration: Option[Config] = configFromPath(configurationFileName, Some("Local configuration file"))

      val classpathConfiguration: Option[Config] =
        (
          for {
            resource <- Try(Source.fromResource(configurationFileName)).recoverWith {
                         case _ => Try(Source.fromResource(s"/$configurationFileName"))
                       }
            config <- Try(ConfigFactory.parseString(resource.mkString))
          } yield config
        ).recoverWith { case _ => Try(ConfigFactory.load(s"/$configurationFileName")) }
          .logSuccess(c => log.info(s"Successfully parsed the classpath configuration at '$configurationFileName'"))
          .logFailure(t => log.error(s"Failed to parse classpath configuration at '$configurationFileName'", t))
          .toOption

      val defaultConfig = Some(
        ConfigFactory
          .load(
            ConfigParseOptions.defaults,
            ConfigResolveOptions.defaults.setAllowUnresolved(true)
          )
      )

      val parametersConf = ConfigFactory.parseString(args.mkString("\n"))

      val configurations =
        Seq(Some(parametersConf), localConfiguration, sparkConfiguration, classpathConfiguration, defaultConfig)

      val fullConfig =
        configurations.collect {
          case Some(config) =>
            config
        }.reduce(_.withFallback(_))
          .withFallback(ConfigFactory.defaultReference())

      val resolvedConfig = Try(fullConfig.resolve())
        .logFailure(_ => log.warn("Failed to resolve the variables locally."))
        .orElse(Try(fullConfig.resolveWith(parametersConf)))
        .logFailure(_ => log.warn(s"Failed to resolve the variables from the arguments."))
        .getOrElse(fullConfig)
      resolvedConfig
    }
  }
}
