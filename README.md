# Spark Utils #


## Motivation ##

One of the biggest challenges after taking the first steps into the world of writing
[Apache Spark](https://spark.apache.org/) applications in Scala is taking them to production.

An application of any kind needs to be easy to run and easy to configure.

This project is trying to help developers write Spark applications focusing mainly on the application logic rather
than the details of configuring the application and setting up the Spark context.


## Description ##

This project contains some basic utilities that can help setting up a Spark application project.

The [`SparkRunnable`](docs/spark-runnable.md) and [`SparkApp`](docs/spark-app.md) together with the
[configuration framework](https://github.com/tupol/scala-utils/blob/master/docs/configuration-framework.md)
provide for easy Spark application creation with configuration that can be managed through configuration files or
application parameters.

The IO frameworks for [reading](docs/data-source.md) and [writing](docs/data-sink.md) data frames add extra convenience
for setting up batch jobs that transform various types of files.

Last but not least, there are many utility functions that provide convenience for loading resources, dealing with
schemas and so on.

Most of the common features are also implemented as *decorators* to main Spark classes, like `SparkContext`, `DataFrame`
and `StructType` and they are conveniently available by importing the `org.tupol.spark.implicits._` package.

The main utilities and frameworks available:
- [SparkApp](docs/spark-app.md) & [SparkRunnable](docs/spark-runnable.md)
- [DataSource Framework](docs/data-source.md)
- [DataSink Framework](docs/data-sink.md)


## Prerequisites ##

* Java 6 or higher
* Scala 2.11 or 2.12
* Apache Spark 2.3.X


## Getting Spark Utils ##

Spark Utils is published to Sonatype OSS and Maven Central:

- Group id / organization: `org.tupol`
- Artifact id / name: `spark-utils`
- Latest version is `0.3.1`

Usage with SBT, adding a dependency to the latest version of tools to your sbt build definition file:

```scala
libraryDependencies += "org.tupol" %% "spark-utils" % "0.3.1"
```

A nice example on how this library can be used can be found in the
[`spark-tools`](https://github.com/tupol/spark-tools) project, through the implementation
of a generic format converter and a SQL processor.


## What's new? ##

**0.3.2-SNAPSHOT**

- Added support for bucketing in data sinks

**0.3.1**

- Added configuration variable substitution support

**0.3.0**

 - Split `SparkRunnable` into `SparkRunnable` and `SparkApp`
 - Changed the `SparkRunnable` API; now `run()` returns `Result` instead of `Try[Result]`
 - Changed the `SparkApp` API; now `buildConfig()` was renamed to `createContext()` and
   now it returns `Context` instead of `Try[Context]`
 - Changed the `DataSource` API; now `read()` returns `DataFrame` instead of `Try[DataFrame]`
 - Changed the `DataSink` API; now `write()` returns `DataFrame` instead of `Try[DataFrame]`
 - Small documentation improvements

**0.2.0**

 - Added `DataSource` and `DataSink` IO frameworks
 - Added `FileDataSource` and `FileDataSink` IO frameworks
 - Added `JdbcDataSource` and `JdbcDataSink` IO frameworks
 - Moved all useful implicit conversions into `org.tupol.spark.implicits`
 - Added testing utilities under `org.tupol.spark.testing`


## What's next? ##

The current development branch is `4.x`. The plan is to bring structured streaming support as well.


## License ##

This code is open source software licensed under the [MIT License](LICENSE).
