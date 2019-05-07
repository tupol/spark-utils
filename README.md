# Spark Utils #

[![Maven Central](https://img.shields.io/maven-central/v/org.tupol/spark-utils_2.11.svg)](https://mvnrepository.com/artifact/org.tupol/spark-utils) &nbsp;
[![GitHub](https://img.shields.io/github/license/tupol/spark-utils.svg)](https://github.com/tupol/spark-utils/blob/master/LICENSE) &nbsp; 
[![Travis (.org)](https://img.shields.io/travis/tupol/spark-utils.svg)](https://travis-ci.com/tupol/spark-utils) &nbsp; 
[![Codecov](https://img.shields.io/codecov/c/github/tupol/spark-utils.svg)](https://codecov.io/gh/tupol/spark-utils) &nbsp;
[![Javadocs](https://www.javadoc.io/badge/org.tupol/spark-utils_2.11.svg)](https://www.javadoc.io/doc/org.tupol/spark-utils_2.11)
[![Gitter](https://badges.gitter.im/spark-utils/spark-utils.svg)](https://gitter.im/spark-utils/spark-utils?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) &nbsp; 
[![Twitter](https://img.shields.io/twitter/url/https/_tupol.svg?color=%2317A2F2)](https://twitter.com/_tupol) &nbsp; 


## Motivation ##

One of the biggest challenges after taking the first steps into the world of writing
[Apache Spark][Spark] applications in [Scala][scala] is taking them to production.

An application of any kind needs to be easy to run and easy to configure.

This project is trying to help developers write Spark applications focusing mainly on the 
application logic rather than the details of configuring the application and setting up the 
Spark context.


## Description ##

This project contains some basic utilities that can help setting up a Spark application project.

The main point is the simplicity of writing Apache Spark applications just focusing on the logic,
while providing for easy configuration and arguments passing.

The code sample bellow shows how easy can be to write a file format converter from any acceptable 
type, with any acceptable parsing configuration options to any acceptable format.

```scala
object FormatConverterExample extends SparkApp[FormatConverterContext, DataFrame] {
  override def createContext(config: Config): FormatConverterContext = 
    FormatConverterContext(config).get
  override def run(implicit spark: SparkSession, context: FormatConverterContext): DataFrame = {
    val inputData = spark.source(context.input).read.sink(context.output).write
  }
}
```

Creating the configuration can be as simple as defining a case class to hold the configuration and
a factory, that helps extract simple and complex data types like input sources and output sinks.

```scala
case class FormatConverterContext(input: FormatAwareDataSourceConfiguration, output: FormatAwareDataSinkConfiguration)

object FormatConverterContext extends Configurator[FormatConverterContext] {
    config.extract[FormatAwareDataSourceConfiguration]("input") |@|
      config.extract[FormatAwareDataSinkConfiguration]("output") apply
      FormatConverterContext.apply
  }
}
```

The [`SparkRunnable`](docs/spark-runnable.md) and [`SparkApp`](docs/spark-app.md) together with the
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
- [SparkApp](docs/spark-app.md) & [SparkRunnable](docs/spark-runnable.md)
- [DataSource Framework](docs/data-source.md) for both batch and structured streaming applications
- [DataSink Framework](docs/data-sink.md) for both batch and structured streaming applications

Latest stable API documentation is available [here](https://www.javadoc.io/doc/org.tupol/spark-utils_2.11/0.3.2).

An extensive tutorial and walk-through can be found [here](https://github.com/tupol/spark-utils-demos/wiki).
Extensive samples and demos can be found [here](https://github.com/tupol/spark-utils-demos).

A nice example on how this library can be used can be found in the
[`spark-tools`](https://github.com/tupol/spark-tools) project, through the implementation
of a generic format converter and a SQL processor for both batch and structured streams.

## Prerequisites ##

* Java 6 or higher
* Scala 2.11 or 2.12
* Apache Spark 2.3.X


## Getting Spark Utils ##

Spark Utils is published to Sonatype OSS and Maven Central:

- Group id / organization: `org.tupol`
- Artifact id / name: `spark-utils`
- Latest version is `0.4.0`

Usage with SBT, adding a dependency to the latest version of tools to your sbt build definition file:

```scala
libraryDependencies += "org.tupol" %% "spark-utils" % "0.4.0"
```


## What's new? ##

**0.4.0**

- Added the `StreamingConfiguration` marker trait
- Added `GenericStreamDataSource`, `FileStreamDataSource` and `KafkaStreamDataSource`
- Added `GenericStreamDataSink`, `FileStreamDataSink` and `KafkaStreamDataSink`
- Added `FormatAwareStreamingSourceConfiguration` and `FormatAwareStreamingSinkConfiguration`
- Extracted `TypesafeConfigBuilder`
- API Changes: Added a new type parameter to the `DataSink` that describes the type of the output
- Improved unit test coverage

For previous versions please consult the [release notes](RELEASE-NOTES.md).


## License ##

This code is open source software licensed under the [MIT License](LICENSE).

[scala]: https://scala-lang.org/
[spark]: https://spark.apache.org/
