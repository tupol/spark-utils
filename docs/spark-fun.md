# SparkFun


## Description

`SparkFun` is a light wrapper around `SparkApp` that allows for a more concise writing of 
Spark applications.

```scala
/**
 * The `contextFactory` is passed in as a constructor argument, the only function needed to be 
 * implemented still being the `run()` function.
 *
 * @param contextFactory the factory function for the application `Context`
 * @tparam Context the type of the application context class.
 * @tparam Result The output type of the run function.
 *
 */
@Experimental
abstract class SparkFun[Context, Result](contextFactory: Config => Try[Context])
  extends SparkApp[Context, Result] {
  final override def createContext(config: Config): Try[Context] = contextFactory(config)
}
```
 
Using this API is fairly easy, and it comes down mainly to defining and implementing one function:
 - `run()` function inherited from the [`SparkRunnable`](spark-runnable.md) trait.
 
The context factory function needs to be passed as an argument to the `SparkFun` while extending it.

For more details check **[`SparkApp`](spark-app.md)**.
