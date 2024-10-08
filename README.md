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
for developers to help each other, so please do not be shy and join through [gitter], [twitter], 
[issue reports](https://github.com/tupol/spark-utils/issues/new/choose) or pull requests.


## ATTENTION!

At the moment there are a lot of changes happening to the `spark-utils` project, hopefully for the better.

The latest stable versions, available through Maven Central are
- Spark 2.4: `0.4.2` to `0.6.2`
- Spark 3.0: `0.6.2` to `1.0.0-RC6`
- Spark >= 3.3.0: `1.0.0-RC7` +

The development version is `1.0.0-R6` which is bringing a clean separation between configuration implementation and the
core, and additionally the [PureConfig] based configuration module that brings the power and features of [PureConfig]
to increase productivity even further and allowing for a more mature configuration framework.

The new modules are:
- `spark-utils-io-pureconfig` for the new [PureConfig] implementation

We completely removed the legacy scalaz based configuration framework.

We suggest to start considering the new for the future `spark-utils-io-pureconfig`.

Migrating to the new `1.0.0-RC6` is quite easy, as the configuration structure was mainly preserved.
More details are available in the [RELEASE-NOTES](RELEASE-NOTES.md).

For now, some of the documentation related or referenced from this project might be obsolete or outdated,
but as the project will get closer to the final release, there will be more improvements. 

### Test Results Matrix

| Spark | Scala 2.12 | Scala 2.13 | Report 1.0.0-RC6                                    | Report 1.0.0-RC7                                    |
|-------|:----------:|:----------:|-----------------------------------------------------|-----------------------------------------------------|
| 3.0.3 |    YES     |    N/A     | [3.0.3](docs/test-results/test_1.0.0-RC6_3.0.3.out) | N/A                                                 |
| 3.1.3 |    YES     |    N/A     | [3.1.3](docs/test-results/test_1.0.0-RC6_3.1.3.out) | N/A                                                 |
| 3.2.4 |    YES     |    YES     | [3.2.4](docs/test-results/test_1.0.0-RC6_3.2.4.out) | N/A                                                 |
| 3.3.4 |    YES     |    YES     | [3.3.4](docs/test-results/test_1.0.0-RC6_3.3.4.out) | [3.3.4](docs/test-results/test_1.0.0-RC7_3.3.4.out) |
| 3.4.2 |    YES     |    YES     | [3.4.2](docs/test-results/test_1.0.0-RC6_3.4.2.out) | [3.4.2](docs/test-results/test_1.0.0-RC7_3.4.2.out) |
| 3.5.1 |    YES     |    YES     | [3.5.1](docs/test-results/test_1.0.0-RC6_3.5.1.out) | [3.5.1](docs/test-results/test_1.0.0-RC7_3.5.1.out) |


## Description ##

This project contains some basic utilities that can help setting up an Apache Spark application project.

The main point is the simplicity of writing Apache Spark applications just focusing on the logic,
while providing for easy configuration and arguments passing.

The code sample bellow shows how easy can be to write a file format converter from any acceptable 
type, with any acceptable parsing configuration options to any acceptable format.

### Batch Application

```scala
import org.tupol.spark._

object FormatConverterExample extends SparkApp[FormatConverterContext, DataFrame] {
  override def createContext(config: Config) = FormatConverterContext.extract(config)
  override def run(implicit spark: SparkSession, context: FormatConverterContext): Try[DataFrame] = {
    val inputData = spark.source(context.input).read
    inputData.sink(context.output).write
  }
}
```

Optionally, the `SparkFun` can be used instead of `SparkApp` to make the code even more concise.

```scala
import org.tupol.spark._

object FormatConverterExample extends 
          SparkFun[FormatConverterContext, DataFrame](FormatConverterContext.extract) {
  override def run(implicit spark: SparkSession, context: FormatConverterContext): Try[DataFrame] = 
    spark.source(context.input).read.sink(context.output).write
}
```

### Configuration

Creating the configuration can be as simple as defining a case class to hold the configuration and
a factory, that helps extract simple and complex data types like input sources and output sinks.

```scala
import org.tupol.spark.io._

case class FormatConverterContext(input: FormatAwareDataSourceConfiguration,
                                  output: FormatAwareDataSinkConfiguration)
```

There are multiple ways that the context can be easily created from configuration files.
This project proposes two ways:
- the new [PureConfig] based framework
- the legacy ScalaZ based framework

#### Configuration creation based on PureConfig

```scala
import com.typesafe.config.Config

object FormatConverterContext {
  import pureconfig.generic.auto._
  import org.tupol.spark.io.pureconf._
  import org.tupol.spark.io.pureconf.readers._
  def extract(config: Config): Try[FormatConverterContext] = config.extract[FormatConverterContext]
}
```

### Streaming Application

For structured streaming applications the format converter might look like this:

```scala
object StreamingFormatConverterExample extends SparkApp[StreamingFormatConverterContext, DataFrame] {
  override def createContext(config: Config) = StreamingFormatConverterContext.extract(config)
  override def run(implicit spark: SparkSession, context: StreamingFormatConverterContext): Try[DataFrame] = {
    val inputData = spark.source(context.input).read
    inputData.streamingSink(context.output).write.awaitTermination()
  }
}
```

### Configuration

The streaming configuration the configuration can be as simple as following:

```scala
import org.tupol.spark.io.streaming.structured._

case class StreamingFormatConverterContext(input: FormatAwareStreamingSourceConfiguration, 
                                           output: FormatAwareStreamingSinkConfiguration)
```

#### Configuration creation based on PureConfig

```scala
object StreamingFormatConverterContext {
  import com.typesafe.config.Config
  import pureconfig.generic.auto._
  import org.tupol.spark.io.pureconf._
  import org.tupol.spark.io.pureconf.streaming.structured._
  def extract(config: Config): Try[StreamingFormatConverterContext] = config.extract[StreamingFormatConverterContext]
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
  - Spark 2.4: `0.4.2` to `0.6.2`
  - Spark 3.0: `0.6.2` to `1.0.0-RC7`
  - Spark 3.3: `1.0.0-RC7` to

Usage with SBT, adding a dependency to the latest version of tools to your sbt build definition file:

```scala
libraryDependencies += "org.tupol" %% "spark-utils-io-pureconfig" % "1.0.0-RC6"
```

Include this package in your Spark Applications using `spark-shell` or `spark-submit`
```bash
$SPARK_HOME/bin/spark-shell --packages org.tupol:spark-utils_2.12:1.0.0-RC6
```


## Starting a New **`spark-utils`** Project ##

***Note*** [spark-utils-g8] was not yet updated for the 1.x version.

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
scalaVersion [2.12.12]:
sparkVersion [3.2.1]:
sparkUtilsVersion [0.4.0]:
```


To fill in the options in advance
```
g8 tupol/spark-apps.seed.g8 --name="My Project" --appname="My App" --organization="my.org" --force
```


## What's new? ##

**1.0.0-RC7**

- Adapt towards the latest Apache Spark versions from 3.3.x
- Added `StreamingTrigger.AvailableNow`
- Build with Spark 3.3.x and tested against Spark 3.3.0 to 3.5.1 

**1.0.0-RC1 to 1.0.0-RC6**

Major library redesign
- Cross compile Scala 2.12 and 2.13
- Building with JDK 17 targeting Java 8
- Added test java options to handle the JDK 17
- Cross compile Scala 2.12 and 2.13
- Build with Spark 3.2.x and tested against Spark 3.x
- Removed the `spark-utils-io-pureconfig` module
- Added configuration module based on [PureConfig]
- `DataSource` exposes `reader` in addition to `read`
- `DataSink` and `DataAwareSink` expose `writer` in addition to `write`
- Added `SparkSessionOps.streamingSource`
- Refactored `TypesafeConfigBuilder`, which has two implementations now: `SimpleTypesafeConfigBuilder` and `FuzzyTypesafeConfigBuilder`
- Small improvements to `SharedSparkSession`
- Documentation improvements  


**0.6.2**

- Fixed `core` dependency to `scala-utils`; now using `scala-utils-core`
- Refactored the `core`/`implicits` package to make the *implicits* a little more *explicit*


For previous versions please consult the [release notes](RELEASE-NOTES.md).


## License ##

This code is open source software licensed under the [MIT License](LICENSE).

[scala]: https://scala-lang.org/
[spark]: https://spark.apache.org/
[spark-utils-g8]: https://github.com/tupol/spark-apps.seed.g8
[maven-central]: https://mvnrepository.com/artifact/org.tupol/spark-utils-core
[spark-packages]: https://spark-packages.org/package/tupol/spark-utils
[license]: https://github.com/tupol/spark-utils/blob/master/LICENSE
[travis.org]: https://travis-ci.com/tupol/spark-utils 
[codecov]: https://codecov.io/gh/tupol/spark-utils
[javadocs]: https://www.javadoc.io/doc/org.tupol/spark-utils_2.12
[gitter]: https://gitter.im/spark-utils/spark-utils
[twitter]: https://twitter.com/_tupol
[PureConfig]: https://pureconfig.github.io/
