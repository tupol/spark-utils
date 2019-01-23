# SparkRunnable


## Description

This is the minimal definition of the Spark application, which only needs a Spark context or session and
some application configuration.

```scala
/**
 * Trivial trait for creating basic runnable Spark applications.
 * These runnable still needs a runner or an app to run.
 *
 * @tparam Configuration the type of the application configuration class.
 * @tparam Result The output type of the run method.
 *
 */
trait SparkRunnable[Configuration, Result]  {

  /**
   * This method needs to be implemented and should contain the entire runnable logic.
   *
   * @param config configuration instance that should contain all the application specific configuration
   * @param spark active spark session
   * @return
   */
  def run(implicit spark: SparkSession, config: Configuration): Try[Result]

}

```
 
Using this API is fairly easy, and it comes down mainly to defining the `run()` method.

One should always think about the return type of the `SparkRunnable` is about to create, though
`SparkRunnable[Config, Result[Unit]]` is also possible.

One can think of the `Configuration` type as the type of the application context.

