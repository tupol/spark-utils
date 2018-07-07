# Configuration Framework


## Scope 

This section is describing the current configuration framework and most importantly the usage patterns.


## Introduction

The configuration framework is based on *ScalaZ* `validation` infrastructure.

The main reason for using the *ScalaZ* `ValidationNel` is to be able to collect all error messages found during the configuration of the target object, and showing them to the user in one go, rather than one by one, as each problem is fixed, application is ran again and the next problem is revealed.

The key concepts are:
- `org.tupol.spark.Configurator`
- `scalaz.ValidationNel`; for more details look [here](https://github.com/scalaz/scalaz/blob/series/7.2.x/core/src/main/scala/scalaz/Validation.scala)
- `com.typesafe.config.Config`; for more details look [here](https://github.com/typesafehub/config)

The `Configurator` is used as a factory for case classes starting from a Typesafe `Config` instance, as shown in the `Configurator` trait itself:

```scala
  import com.typesafe.config.Config
  import scalaz.ValidationNel
  
  trait Configurator[T] {
    def validationNel(config: Config): ValidationNel[Throwable, T]
    def apply(config: Config): Try[T] = validationNel(config)
  }
```
The type parameter `T` is the type of the class instance we are constructing.



## Usage

In order to create a new configuration we need the following:
1. A case class to configure or select an existing one
2. A corresponding `Configurator` containing an implementation of the `validationNel()` function 

### Simple Configuration Example

The full example is available in [`examples.MySimpleExample`](src/test/scala/examples/MySimpleExample.scala).

#### 1. Create a case class to configure or select an existing one

The normal pattern we used so far is to create a configuration case class that will only hold the configuration of the class we actually want to configure.
This is not the only way of doing it, and we can configure any case class directly (see the `NaiveProbifier`).

Let's say that we have an example class that we need to configure, like bellow:

```scala
case class MySimpleExample(path: String, overwrite: Boolean) {
  def write(something: String) = {
    // Some code using the path and the overwrite parameters.
    //...
    println(s"""Writing "$something" to "$path" with the overwrite flag set to "$overwrite".""")
  }
}
```

#### 2. Create a corresponding `Configurator` and implement the `validationNel()` function 

```scala
import org.tupol.spark.config.Configurator

object MySimpleExample extends Configurator[MySimpleExample] {

  import com.typesafe.config.Config
  import scalaz.ValidationNel

  override def validationNel(config: Config): ValidationNel[Throwable, MySimpleExample] = {

    import org.tupol.spark.config._
    import scalaz.syntax.applicative._

    config.extract[String]("path") |@| config.extract[Boolean]("overwrite") apply MySimpleExample.apply
  }
}
```



***Notes***

1. Normally we define the configurator factory in a companion object attached to the case class that we want to configure.

2. The only top level import is `import org.tupol.spark.config.Configurator`.
   We are quite keen on keeping the top level imports clean, and only import locally what we need.
   The subsequent sets of imports are inside the `MySimpleExample` object scope and in the `validationNel` function scope.

3. We only need to implement the `validationNel` function.

4. Notice the `config.extract[String]("path")` function call. What happens here is the following:
    - the `config` is implicitly converted to a `RichConfig` instance, that uses the *type class* pattern in the `extract` function
    - the actual type classes can be found inside the `Extractor` object, all available in the `org.tupol.spark.config` package
    - the `path` refers to a path inside the `config` object passed in as an argument
    - the `[String]` type parameter is used to trigger the type class matching
    - the `RichConfig.extract` function returns `ValidationNel[Throwable, T]`
    - to conclude, each `config.extract[T]("configuration_path")` returns a `ValidationNel[Throwable, T]`

5. Notice the `|@|` operator.
    - the `|@|` operator is used to compose `ValidationNel[Throwable, T]`s and create an `ApplicativeBuilder` 
    - on the `ApplicativeBuilder` created  we can call `apply` and pass in a function that creates our object, `MySimpleExample`
    - for a case class or for smart constructor in companion objects, such an function is `apply`
    - the order of the `config.extract[T]("configuration_path")` is very important and it needs to match the type signature of the case class we are trying to construct;
      In our example we can not change the order of `config.extract[String]("path")` and `config.extract[Boolean]("overwrite")` because composing them will create a tuple of Boolean and String and there is no such constructor available in `MySimpleExample`
    - in case our target class has multiple constructors, we need to specify the constructor better; for example we can use something like `MyComplexExample.apply(_, _, _)`
    - the final result of the `validationNel` function is a `ValidationNel[Throwable, T]`

6. When actually building a configured instance, the `apply` function will be used, that yields a `Try[T]`.
    - the returned try may contain either the successfully configured instance or a failure containing a `ConfigurationException` 
    - the `ConfigurationException` wraps all the `Throwables` accumulated by the `ValidationNel` and has a human friendly message that lists all the problems.


### Composite Configuration Example

Just as described previously, we can also create configuration objects that are based on other configuration objects.

The full example is available in [`examples.MyComplexExample`](src/test/scala/examples/MyComplexExample.scala).


#### 1. Create a case class to configure or select an existing one

```scala
case class MyComplexExample(example: MySimpleExample, separatorChar: String, separatorSize: Int) {
  def write(something: String) = {
    // Some code using the path and the overwrite parameters.
    //...
    val separator = (0 until separatorSize).map(_ => separatorChar).mkString("")
    println(separator)
    println(s"""Writing "$something" to "${example.path}" with the overwrite flag set to "${example.overwrite}".""")
  }
}
```

***Notes***

1. Notice that `MyComplexExample` has as a class parameter a `MySimpleExample`.


#### 2. Create a corresponding `Configurator` and implement the `validationNel()` function 

```scala
import org.tupol.spark.config.Configurator

object MyComplexExample extends Configurator[MyComplexExample] {

  import com.typesafe.config.Config
  import scalaz.ValidationNel

  override def validationNel(config: Config): ValidationNel[Throwable, MyComplexExample] = {

    import org.tupol.spark.config._
    import scalaz.syntax.applicative._
    import com.typesafe.config.ConfigException.BadValue

    val separatorChar = config.extract[String]("separatorChar").
      ensure(new BadValue("separatorChar", "should be a single character.").toNel)(t => t.length == 1)

    val separatorSize = config.extract[Int]("separatorSize").
      ensure(new IllegalArgumentException("The separatorSize should be between 1 and 80.").toNel)(s => s > 0 && s <= 80)

    MySimpleExample.validationNel(config) |@| separatorChar |@| separatorSize apply MyComplexExample.apply
  }
}
```


***Notes***

1. Notice the way the `separatorChar` and `separatorSize` are defined outside the `ApplicativeBuilder` composition line.

2. Notice that both `separatorChar` and `separatorSize` have extra validation rules implemented through the `ensure` function.
    - the `ensure` function 
        - takes as a first argument a function that creates a `Throwable` and 
        - takes as a second argument a predicate that is applied to the value contained in the `ValidationNel`
        - if testing the value against the predicate fails, then the `Throwable` defined as a first parameter is added to the `NonEmptyList[Throwable]` of the `ValidationNel`
        - for more details don't hesitate to check the [scalaz documentation](https://github.com/scalaz/scalaz/blob/series/7.2.x/core/src/main/scala/scalaz/Validation.scala)
    - the `Throwable`s are implicitly converted to `NonEmptyList[Throwable]` by the `org.tupol.spark.config._` package defined implicits;
      This way the type signature `ValidationNel[Throwable, T]` is preserved.
      
3. For the final line to work, `MySimpleExample.validationNel(config)` needs to be defined.

