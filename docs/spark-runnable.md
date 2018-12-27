# SparkRunnable


## Description

This is the main workhorse for our Spark applications, providing most of the infrastructure for building a Spark application.

```scala

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

  ....
  
}
```
 
Using this API is fairly easy, and it comes down mainly to defining the `run()` method.
One should always think about the return type of the `SparkRunnable` is about to create, though `SparkRunnable[Config, Result[Unit]]` is also possible.
It is expected for the user to define a friendly `appName` that will be used as a configuration path marker.


## Configuration

The `SparkRunnable` configuration is done through the `config: Config` parameter of the `run()` method. 

The `SparkRunnable` also has a `main()` method, which can be used to pass application parameters.
Using the `main()` method, configuration is passed in the following ways, in the order specified bellow:
1. Application parameters; they are passed in a properties style, separated by whitespaces, like `app.name.param1=param1value app.name.param2=param2value`.
2. Configuration file; passed as an argument to `spark-submit --files=..../application.conf`
3. Configuration file `application.conf`, if available in the classpath.
4. Reference configuration file; sometimes available in the application jar itself as `reference.conf`.
The order is important because the a parameter defined in the application parameters overwrites the parameter with the same name defined in the application.conf, which in turn overwrites the parameter with the same name from the reference.conf.

The `application.conf` and the `reference.conf` are acceptable in properties, json or HOCON formats. See also the [Typesafe Config](https://github.com/typesafehub/config) project for more details.


## Logging

`SparkRunnable` extends Logging, so any implementation will have logging available out of the box.

However, one should be careful while setting the logging level in order not to pollute the logs with debugging information for example.

