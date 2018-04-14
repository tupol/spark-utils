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
 * @param example
 * @param separatorChar some line separator character
 * @param separatorSize how many times should the `separatorChar` should be multiplied in forming the separator line
 */
case class MyComplexExample(example: MySimpleExample, separatorChar: String, separatorSize: Int) {
  def write(something: String) = {
    // Some code using the path and the overwrite parameters.
    //...
    val separator = (0 until separatorSize).map(_ => separatorChar).mkString("")
    println(separator)
    println(s"""Writing "$something" to "${example.path}" with the overwrite flag set to "${example.overwrite}".""")
  }
}

import io.tupol.spark.config._

/**
 * This is the configurator implementation for the MyExample case class
 */
object MyComplexExample extends Configurator[MyComplexExample] {

  import com.typesafe.config.Config

  import scalaz.ValidationNel

  override def validationNel(config: Config): ValidationNel[Throwable, MyComplexExample] = {

    import com.typesafe.config.ConfigException.BadValue

    import scalaz.syntax.applicative._

    val separatorChar = config.extract[String]("separatorChar").
      ensure(new BadValue("separatorChar", "should be a single character.").toNel)(t => t.length == 1)

    val separatorSize = config.extract[Int]("separatorSize").
      ensure(new IllegalArgumentException("The separatorSize should be between 1 and 80.").toNel)(s => s > 0 && s <= 80)

    MySimpleExample.validationNel(config) |@| separatorChar |@| separatorSize apply MyComplexExample.apply
  }
}

/**
 * This is a simple example on how to use the
 */
object MyComplexExampleDemo extends App {

  import com.typesafe.config.ConfigFactory
  val goodConfig = ConfigFactory.parseString(
    """
      |myExample.path="some_path"
      |myExample.overwrite=true
      |myExample.separatorChar="*"
      |myExample.separatorSize=15
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
  val myExample = MyComplexExample(goodConfig.getConfig("myExample")).get
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
      |myExample: {
      |  path="some_path"
      |  separatorChar="--"
      |  separatorSize=81
      |}
    """.stripMargin
  )
  print(MyComplexExample(wrongConfig.getConfig("myExample")).recover { case (t: Throwable) => t.getMessage }.get)
}

