package org.tupol.spark

import org.apache.spark.annotation.Experimental

import scala.util.Try

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
@Experimental
abstract class SparkFun[Context, Result](contextFactory: Array[String] => Try[Context]) extends SparkApp[Context, Result] {
  final override def createContext(args: Array[String]): Try[Context] = contextFactory(args)
}
