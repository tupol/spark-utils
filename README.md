# Spark Utils #

[![Maven Central](https://img.shields.io/maven-central/v/org.tupol/spark-utils_2.12.svg)][maven-central] &nbsp;
[![GitHub](https://img.shields.io/github/license/tupol/spark-utils.svg)][license] &nbsp;
[![Travis (.org)](https://img.shields.io/travis/tupol/spark-utils.svg)][travis.org] &nbsp;
[![Codecov](https://img.shields.io/codecov/c/github/tupol/spark-utils.svg)][codecov] &nbsp;
[![Javadocs](https://www.javadoc.io/badge/org.tupol/spark-utils_2.12.svg)][javadocs] &nbsp;
[![Gitter](https://badges.gitter.im/spark-utils/spark-utils.svg)][gitter] &nbsp;
[![Twitter](https://img.shields.io/twitter/url/https/_tupol.svg?color=%2317A2F2)][twitter] &nbsp;


## Motivation ##

One of the biggest challenges after taking the first steps into the world of writing
[Apache Spark][Spark] applications in [Scala][scala] is taking them to production.

An application of any kind needs to be easy to run and easy to configure.

This project is trying to help developers write Spark applications focusing mainly on the 
application logic rather than the details of configuring the application and setting up the 
Spark context.

This project is also trying to create and encourage a friendly yet professional environment 
for developers to help each other, so please do no be shy and join through [gitter], [twitter], 
[issue reports](https://github.com/tupol/spark-utils/issues/new/choose) or pull requests.


## Description ##

This project contains some basic utilities that can help setting up a Spark application project.

The main point is the simplicity of writing Apache Spark applications just focusing on the logic,
while providing for easy configuration and arguments passing.

The code sample bellow shows how easy can be to write a file format converter from any acceptable 
type, with any acceptable parsing configuration options to any acceptable format.

```scala
object FormatConverterExample extends SparkApp[FormatConverterContext, DataFrame] {
  override def createContext(config: Config) = FormatConverterContext(config)
  override def run(implicit spark: SparkSession, context: FormatConverterContext): Try[DataFrame] = {
    val inputData = spark.source(context.input).read
    inputData.sink(context.output).write
  }
}
```

Creating the configuration can be as simple as defining a case class to hold the configuration and
a factory, that helps extract simple and complex data types like input sources and output sinks.

```scala
case class FormatConverterContext(input: FormatAwareDataSourceConfiguration,
                                  output: FormatAwareDataSinkConfiguration)

object FormatConverterContext extends Configurator[FormatConverterContext] {
  import com.typesafe.config.Config
  import scalaz.ValidationNel

  def validationNel(config: Config): ValidationNel[Throwable, FormatConverterContext] = {
    import scalaz.syntax.applicative._
    config.extract[FormatAwareDataSourceConfiguration]("input") |@|
      config.extract[FormatAwareDataSinkConfiguration]("output") apply
      FormatConverterContext.apply
  }
}
```

Optionally, the `SparkFun` can be used instead of  `SparkApp` to make the code even more concise.

```scala
object FormatConverterExample extends 
          SparkFun[FormatConverterContext, DataFrame](FormatConverterContext(_).get) {
  override def run(implicit spark: SparkSession, context: FormatConverterContext): Try[DataFrame] = 
    spark.source(context.input).read.sink(context.output).write
}
```


For structured streaming applications the format converter might look like this:

```scala
object StreamingFormatConverterExample extends SparkApp[StreamingFormatConverterContext, DataFrame] {
  override def createContext(config: Config) = StreamingFormatConverterContext(config).get
  override def run(implicit spark: SparkSession, context: StreamingFormatConverterContext): Try[DataFrame] = {
    val inputData = spark.source(context.input).read
    inputData.streamingSink(context.output).write.awaitTermination()
  }
}
```

The streaming configuration the configuration can be as simple as following:

```scala
case class StreamingFormatConverterContext(input: FormatAwareStreamingSourceConfiguration, 
                                           output: FormatAwareStreamingSinkConfiguration)

object StreamingFormatConverterContext extends Configurator[StreamingFormatConverterContext] {
  def validationNel(config: Config): ValidationNel[Throwable, StreamingFormatConverterContext] = {
    config.extract[FormatAwareStreamingSourceConfiguration]("input") |@|
      config.extract[FormatAwareStreamingSinkConfiguration]("output") apply
      StreamingFormatConverterContext.apply
  }
}
```

The [`SparkRunnable`](docs/spark-runnable.md) and [`SparkApp`](docs/spark-app.md) or 
[`SparkFun`](docs/spark-fun.md) together with the 
[configuration framework](https://github.com/tupol/scala-utils/blob/master/docs/configuration-framework.md)
provide for easy Spark application creation with configuration that can be managed through 
configuration files or application parameters.

The IO frameworks for [reading](docs/data-source.md) and [writing](docs/data-sink.md) data frames 
add extra convenience for setting up batch and structured streaming jobs that transform 
various types of files and streams.

Last but not least, there are many utility functions that provide convenience for loading 
resources, dealing with schemas and so on.

Most of the common features are also implemented as *decorators* to main Spark classes, like
`SparkContext`, `DataFrame` and `StructType` and they are conveniently available by importing 
the `org.tupol.spark.implicits._` package.


## Documentation ##
The documentation for the main utilities and frameworks available:
- [SparkApp](docs/spark-app.md), [SparkFun](docs/spark-fun.md) and [SparkRunnable](docs/spark-runnable.md)
- [DataSource Framework](docs/data-source.md) for both batch and structured streaming applications
- [DataSink Framework](docs/data-sink.md) for both batch and structured streaming applications

Latest stable API documentation is available [here](https://www.javadoc.io/doc/org.tupol/spark-utils_2.12/0.4.2).

An extensive tutorial and walk-through can be found [here](https://github.com/tupol/spark-utils-demos/wiki).
Extensive samples and demos can be found [here](https://github.com/tupol/spark-utils-demos).

A nice example on how this library can be used can be found in the
[`spark-tools`](https://github.com/tupol/spark-tools) project, through the implementation
of a generic format converter and a SQL processor for both batch and structured streams.


## Prerequisites ##

* Java 8 or higher
* Scala 2.12
* Apache Spark 3.0.X


## Getting Spark Utils ##

Spark Utils is published to [Maven Central][maven-central] and [Spark Packages][spark-packages]:

- Group id / organization: `org.tupol`
- Artifact id / name: `spark-utils`
- Latest stable versions:
  - Spark 2.4: `0.4.2`
  - Spark 3.0: `0.6.1`

Usage with SBT, adding a dependency to the latest version of tools to your sbt build definition file:

```scala
libraryDependencies += "org.tupol" %% "spark-utils" % "0.6.1"
```

Include this package in your Spark Applications using `spark-shell` or `spark-submit`
```bash
$SPARK_HOME/bin/spark-shell --packages org.tupol:spark-utils_2.12:0.4.2
```


## Starting a New **`spark-utils`** Project ##

The simplest way to start a new `spark-utils` is to make use of the 
[`spark-apps.seed.g8`][spark-utils-g8] template project.


To fill in manually the project options run
```
g8 tupol/spark-apps.seed.g8
```

The default options look like the following:
```
name [My Project]:
appname [My First App]:
organization [my.org]:
version [0.0.1-SNAPSHOT]:
package [my.org.my_project]:
classname [MyFirstApp]:
scriptname [my-first-app]:
scalaVersion [2.11.12]:
sparkVersion [2.4.0]:
sparkUtilsVersion [0.4.0]:
```


To fill in the options in advance
```
g8 tupol/spark-apps.seed.g8 --name="My Project" --appname="My App" --organization="my.org" --force
```


## What's new? ##

**0.6.1**

- Small dependencies and documentation improvements
- The documentation needs to be further reviewed
- The project is split into two modules: `spark-utils-core` and `spark-utils-io`
- The project moved to Apache Spark 3.0.1, which is a popular choice for the Databricks Cluster users
- The project is only compiled on Scala 2.12
- There is a major redesign of core components, mainly returning `Try[_]` for better exception handling
- Dependencies updates

For previous versions please consult the [release notes](RELEASE-NOTES.md).


## License ##

This code is open source software licensed under the [MIT License](LICENSE).

[scala]: https://scala-lang.org/
[spark]: https://spark.apache.org/
[spark-utils-g8]: https://github.com/tupol/spark-apps.seed.g8
[maven-central]: https://mvnrepository.com/artifact/org.tupol/spark-utils
[spark-packages]: https://spark-packages.org/package/tupol/spark-utils
[license]: https://github.com/tupol/spark-utils/blob/master/LICENSE
[travis.org]: https://travis-ci.com/tupol/spark-utils 
[codecov]: https://codecov.io/gh/tupol/spark-utils
[javadocs]: https://www.javadoc.io/doc/org.tupol/spark-utils_2.12
[gitter]: https://gitter.im/spark-utils/spark-utils
[twitter]: https://twitter.com/_tupol
