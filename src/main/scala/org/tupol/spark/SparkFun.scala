package org.tupol.spark

import com.typesafe.config.Config

/**
 * `SparkFun` is a light wrapper around `SparkApp` that allows for a more concise writing of Spark applications.
 *
 * The `contextFactory` is passed in as a constructor argument, the only function needed to be implemented still
 * being the `run()` function.
 *
 * @param contextFactory the factory function for the application `Context`
 * @tparam Context the type of the application context class.
 * @tparam Result The output type of the run function.
 *
 */
abstract class SparkFun[Context, Result](contextFactory: Config => Context)
  extends SparkApp[Context, Result] {
  final override def createContext(config: Config): Context = contextFactory(config)
}
