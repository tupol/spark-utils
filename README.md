# Spark Utils

This project contains some basic utilities that can help setting up a Spark project.

The [`SparkRunnable`](docs/spark-runnable.md), together with the [configuration framework](docs/configuration-framework.md)
provide for easy Spark application creation with configuration that can be managed through configuration files or
application parameters.

The IO frameworks for [loading](docs/file-data-frame-loader.md) and [saving](docs/file-data-frame-saver.md) data frames
add extra convenience for setting up batch jobs that transform various types of files.

Last but not least, there are many utility functions that provide convenience for loading resources, dealing with schemas
and so on.

Most of the common features are also implemented as *decorators* to main Spark classes, like `SparkContext`, `DataFrame`
and `StructType` and they are conveniently available by importing the `org.tupol.spark.implicits._` package.

The main utilities and frameworks available:
- [SparkRunnable](docs/spark-runnable.md)
- [Configuration Framework](docs/configuration-framework.md)
- [FileDataFrameLoader Framework](docs/file-data-frame-loader.md)
- [FileDataFrameSaver Framework](docs/file-data-frame-saver.md)


## Prerequisites ##

* Java 6 or higher
* Scala 2.11,or 2.12
* Apache Spark 2.3.X


## Getting Spark Utils ##

Spark Utils is published to Sonatype OSS and Maven Central:

- Group id / organization: `org.tupol`
- Artifact id / name: `spark-utils`
- Latest version is `0.2.0-SNAPSHOT`

Usage with SBT, adding a dependency to the latest version of tools to your sbt build definition file:

```scala
libraryDependencies += "org.tupol" %% "spark-utils" % "0.2.0-SNAPSHOT"
```


## What's new? ##

### 0.2.0-SNAPSHOT ###
 - Added `FileDataFrameLoader` and `FileDataFrameSaver` IO frameworks
 - Moved all useful implicit conversions into `org.tupol.spark.implicits`
 - Added testing utilities under `org.tupol.spark.testing`


## License ##

This code is open source software licensed under the [MIT License](LICENSE).
