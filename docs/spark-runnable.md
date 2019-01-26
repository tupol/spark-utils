# SparkRunnable


## Description

This is the minimal definition of the Spark application, which only needs a Spark context or session and
some application configuration.

```scala
/**
 * Trivial trait for creating basic runnable Spark applications.
 * These runnable still needs a runner or an app to run.
 *
 * @tparam Context the type of the application context class.
 * @tparam Result The output type of the run function.
 *
 */
trait SparkRunnable[Context, Result] {

  /**
   * This function needs to be implemented and should contain the entire runnable logic.
   *
   * @param context context instance that should contain all the application specific configuration
   * @param spark active spark session
   * @return
   */
  def run(implicit spark: SparkSession, context: Context): Result

}

```
 
Using this API is fairly easy, and it comes down mainly to defining the `run()` function.

One should always think about the return type of the `SparkRunnable` is about to create, though
`SparkRunnable[Config, Unit]` is also possible.

The `Context` can be seen as the application configuration and in most of the cases the most basic case class will suffice.

The main use of the `SparkRunnable` is through the implementation of the [`SparkApp`](spark-app.md) trait.
