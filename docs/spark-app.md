# SparkApp


## Description

This is the main workhorse for our Spark applications, providing most of the infrastructure for building
a Spark application.

```scala
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
   * the form of 'APP_NAME_....', reflected also in the configuration structure.
   *
   * By default this will return the simple class name.
   */
  def appName: String = getClass.getSimpleName.replaceAll("\\$", "")

  /**
   * This function needs to be implemented and should contain all logic related
   * to parsing the configuration settings and building the application context.
   */
  def createContext(config: Config): Try[Context]


}
```
 
Using this API is fairly easy, and it comes down mainly to defining and implementing two functions:
 - `run()` function inherited from the [`SparkRunnable`](spark-runnable.md) trait;
 - `createContext()` function that creates the application configuration out of the given
    [Typesafe `Config`](https://github.com/lightbend/config/blob/master/config/src/main/java/com/typesafe/config/Config.java)
    instance.

One should always think about the return type of the `SparkApp` is about to create, though
`SparkApp[Context, Result[Unit]]` is also possible.
It is expected for the user to define a friendly `appName` that will be used as a configuration path marker, but a
default name, consisting of the simple class name is provided as an application name.

The application is actually executed by the `main()` function, which makes any application implementing `SparkApp`
an actual Spark application. The following steps will be performed:

1. Configuration initialization and application context creation
2. Spark session creation
3. Execution of the `run()` function
4. Logging the steps and the success or failure of the application; including the entire configuration used.
5. Return the result and exit


## Configuration

The `SparkApp` configuration is done through the `config: Config` parameter of the `run()` function.

The `SparkApp` also has a `main()` function, which can be used to pass application parameters.
By default `SparkApp` is using the `FuzzyTypesafeConfigBuilder` so when using the `main()` function, the configuration 
is passed in the following ways, in the order specified bellow:

1. Application parameters; they are passed in a properties style, separated by whitespaces, like
   `app.name.param1=param1value app.name.param2=param2value`.
2. Configuration file; passed as an argument to `spark-submit --files=..../application.conf`
3. Configuration file `application.conf`, if available in the classpath.
4. Reference configuration file; sometimes available in the application jar itself as `reference.conf`.

The order is important because a parameter defined in the application parameters overwrites 
the parameter with the same name defined in the application.conf, which in turn overwrites the
parameter with the same name from the `reference.conf`.

The `application.conf` and the `reference.conf` are acceptable in Java properties, Json or HOCON
formats.
See also the [Typesafe Config](https://github.com/typesafehub/config) project for more details.

The way the configuration is loaded can be changed, by changing the implementation of the `getConfiguration` method.

```scala
override def getConfiguration(args: Array[String], configurationFileName: String): Try[Config]
```

For example, it can be overwritten to use the `SimpleTypesafeConfigBuilder` implementation or any other custom 
implementation.

## Logging

`SparkApp` extends Logging, so any implementation will have logging available out of the box.

However, one should be careful while setting the logging level in order not to pollute the logs 
with debugging information for example.

