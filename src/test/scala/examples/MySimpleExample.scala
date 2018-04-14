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
package examples

// This is an example on how the configuration framework can be used.

/**
 * This is the class that we want to configure.
 *
 * @param path
 * @param overwrite
 */
case class MySimpleExample(path: String, overwrite: Boolean) {
  def write(something: String) = {
    // Some code using the path and the overwrite parameters.
    //...
    println(s"""Writing "$something" to "$path" with the overwrite flag set to "$overwrite".""")
  }
}

import io.tupol.spark.config._

/**
 * This is the configurator implementation for the MySimpleExample case class
 */
object MySimpleExample extends Configurator[MySimpleExample] {

  import com.typesafe.config.Config
  import scalaz.ValidationNel

  override def validationNel(config: Config): ValidationNel[Throwable, MySimpleExample] = {

    import scalaz.syntax.applicative._

    config.extract[String]("path") |@| config.extract[Boolean]("overwrite") apply MySimpleExample.apply
  }
}

/**
 * This is a simple example on how to use the
 */
object MySimpleExampleDemo extends App {

  import com.typesafe.config.ConfigFactory
  val goodConfig = ConfigFactory.parseString(
    """
      |myExample.path="some_path"
      |myExample.overwrite=true
    """.stripMargin
  )

  println(
    s"""===============
        | Positive Test
        |===============""".stripMargin
  )
  // This is the place where the "magic happens" and everything goes well
  // Notice that we extract the exact configuration that we need out of the root configuration object
  // by calling `goodConfig.getConfig("myExample")`
  val myExample = MySimpleExample(goodConfig.getConfig("myExample")).get
  myExample.write("A quick brown fox...")

  println(
    s"""|
        |===============
        | Negative Test
        |===============""".stripMargin
  )
  // This is where we see what happens when there are some problems
  val wrongConfig = ConfigFactory.parseString(
    """
      |myExample: {}
    """.stripMargin
  )
  print(MySimpleExample(wrongConfig.getConfig("myExample")).recover { case (t: Throwable) => t.getMessage }.get)
}

