# SparkRunnable


## Description

This is the minimal definition of the Spark application, which only needs a Spark context or session and
some application configuration.

```scala
/**
 * Trivial trait for creating basic runnable Spark applications.
 * These runnable still needs a runner or an app to run.
 *
 * @tparam Context the type of the application context or configuration class.
 * @tparam Result The output type of the run method.
 *
 */
trait SparkRunnable[Context, Result] {

  /**
   * This method needs to be implemented and should contain the entire runnable logic.
   *
   * @param context context instance that should contain all the application specific configuration
   * @param spark active spark session
   * @return
   */
  def run(implicit spark: SparkSession, context: Context): Result

}

```
 
Using this API is fairly easy, and it comes down mainly to defining the `run()` method.

One should always think about the return type of the `SparkRunnable` is about to create, though
`SparkRunnable[Config, Unit]` is also possible.

The `Context` can be seen as the application configuration and in most of the cases the most basic case class will suffice.
