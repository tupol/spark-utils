# Release Notes

## 0.6

**0.6.2**

- Fixed `core` dependency to `scala-utils`; now using `scala-utils-core`
- Refactored the `core`/`implicits` package to make the *implicits* a little more *explicit*

**0.6.1**

- Small dependencies and documentation improvements
- The documentation needs to be further reviewed
- The project is split into two modules: `spark-utils-core` and `spark-utils-io`
- The project moved to Apache Spark 3.0.1, which is a popular choice for the Databricks Cluster users
- The project is only compiled on Scala 2.12
- There is a major redesign of core components, mainly returning `Try[_]` for better exception handling
- Dependencies updates


## 0.5

**N/A**


## 0.4

**0.4.2**

- The project compiles with both Scala `2.11.12` and `2.12.12`
- Updated Apache Spark to `2.4.6`
- Updated the `spark-xml` library to `0.10.0`
- Removed the `com.databricks:spark-avro` dependency, as avro support is now built into Apache Spark
- Removed the shadow `org.apache.spark.Loggin` class, which is replaced by the `org.tupol.spark.Loggign` knock-off

**0.4.1**

- Added [`SparkFun`](docs/spark-fun.md), a convenience wrapper around 
  [`SparkApp`](docs/spark-app.md) that makes the code even more concise
- Added ` FormatType.Custom` so any format types are accepted, but of course, not any 
  random format type will work, but now other formats like 
  [`delta`](https://github.com/delta-io/delta) can be configured and used
- Added `GenericSourceConfiguration` (replacing the old private `BasicConfiguration`) 
  and `GenericDataSource` 
- Added `GenericSinkConfiguration`, `GenericDataSink` and  `GenericDataAwareSink`
- Removed the short `”avro”` format as it will be included in Spark 2.4
- Added format validation to `FileSinkConfiguration`
- Added [generic-data-source.md](docs/generic-data-source.md) and [generic-data-sink.md](docs/generic-data-sink.md) docs

**0.4.0**

- Added the `StreamingConfiguration` marker trait
- Added `GenericStreamDataSource`, `FileStreamDataSource` and `KafkaStreamDataSource`
- Added `GenericStreamDataSink`, `FileStreamDataSink` and `KafkaStreamDataSink`
- Added `FormatAwareStreamingSourceConfiguration` and `FormatAwareStreamingSinkConfiguration`
- Extracted `TypesafeConfigBuilder`
- API Changes: Added a new type parameter to the `DataSink` that describes the type of the output
- Improved unit test coverage


## 0.3

**0.3.2**

- Added support for bucketing in data sinks
- Improved the community resources

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


## 0.2

**0.2.0**

 - Added `DataSource` and `DataSink` IO frameworks
 - Added `FileDataSource` and `FileDataSink` IO frameworks
 - Added `JdbcDataSource` and `JdbcDataSink` IO frameworks
 - Moved all useful implicit conversions into `org.tupol.spark.implicits`
 - Added testing utilities under `org.tupol.spark.testing`
