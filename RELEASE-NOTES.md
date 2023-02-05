# Release Notes

[[__TOC__]]

## 1.0

_Soon to a repo near you_ :)

### 1.0.0-RC3

- `DataSource` exposes `reader` in addition to `read`
- Added `SparkSessionOps.streamingSource`

### 1.0.0-RC2

- `DataSink` and `DataAwareSink` expose `writer` in addition to `write`
- Documentation improvements

### 1.0.0-RC1

**Major Library Redesign**

The project was split into different configuration modules
- `spark-utils-io-pureconfig` for the new [PureConfig] implementation
- `spark-utils-io-configz` for the legacy ConfigZ implementation

#### Migration notes

##### Dependencies

It is best to import either one of the following 
- `"org.tupol" %% "spark-utils-io-configz"    % sparkUtilsVersion`
- `"org.tupol" %% "spark-utils-io-pureconfig" % sparkUtilsVersion`

instead of

- `"org.tupol" %% "spark-utils"               % sparkUtilsVersion`

##### Configuration parameters
- `kafka.bootstrap.servers` was renamed to `kafkaBootstrapServers` in Kafka sources and sinks configuration
- `bucketColumns` was renamed to `columns` in file data sinks
- `partition.files` was renamed to `partition.number` in sinks configuration

##### Others
- `SourceConfiguration.extract` is no longer used; use `SourceConfigurator.extract` instead
- `FileSourceConfiguration.extract` is no longer used; use `FileSourceConfigurator.extract` instead
- `GenericSinkConfiguration.optionalSaveMode` was renamed to `GenericSinkConfiguration.mode`
- `TypesafeConfigBuilder.applicationConfiguration()` was renamed to `getApplicationConfiguration()` and was made public,
   so it can be overridden and the `args` is no longer an implicit parameter; This impacts `SparkApp` and `SparkFun`


## 0.6

### 0.6.2

- Fixed `core` dependency to `scala-utils`; now using `scala-utils-core`
- Refactored the `core`/`implicits` package to make the *implicits* a little more *explicit*

### 0.6.1

- Small dependencies and documentation improvements
- The documentation needs to be further reviewed
- The project is split into two modules: `spark-utils-core` and `spark-utils-io`
- The project moved to Apache Spark 3.0.1, which is a popular choice for the Databricks Cluster users
- The project is only compiled on Scala 2.12
- There is a major redesign of core components, mainly returning `Try[_]` for better exception handling
- Dependencies updates


## 0.5

### N/A


## 0.4

### 0.4.2

- The project compiles with both Scala `2.11.12` and `2.12.12`
- Updated Apache Spark to `2.4.6`
- Updated the `spark-xml` library to `0.10.0`
- Removed the `com.databricks:spark-avro` dependency, as avro support is now built into Apache Spark
- Removed the shadow `org.apache.spark.Loggin` class, which is replaced by the `org.tupol.spark.Loggign` knock-off

### 0.4.1

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

### 0.4.0

- Added the `StreamingConfiguration` marker trait
- Added `GenericStreamDataSource`, `FileStreamDataSource` and `KafkaStreamDataSource`
- Added `GenericStreamDataSink`, `FileStreamDataSink` and `KafkaStreamDataSink`
- Added `FormatAwareStreamingSourceConfiguration` and `FormatAwareStreamingSinkConfiguration`
- Extracted `TypesafeConfigBuilder`
- API Changes: Added a new type parameter to the `DataSink` that describes the type of the output
- Improved unit test coverage


## 0.3

### 0.3.2

- Added support for bucketing in data sinks
- Improved the community resources

### 0.3.1

- Added configuration variable substitution support

### 0.3.0

 - Split `SparkRunnable` into `SparkRunnable` and `SparkApp`
 - Changed the `SparkRunnable` API; now `run()` returns `Result` instead of `Try[Result]`
 - Changed the `SparkApp` API; now `buildConfig()` was renamed to `createContext()` and
   now it returns `Context` instead of `Try[Context]`
 - Changed the `DataSource` API; now `read()` returns `DataFrame` instead of `Try[DataFrame]`
 - Changed the `DataSink` API; now `write()` returns `DataFrame` instead of `Try[DataFrame]`
 - Small documentation improvements


## 0.2

### 0.2.0

 - Added `DataSource` and `DataSink` IO frameworks
 - Added `FileDataSource` and `FileDataSink` IO frameworks
 - Added `JdbcDataSource` and `JdbcDataSink` IO frameworks
 - Moved all useful implicit conversions into `org.tupol.spark.implicits`
 - Added testing utilities under `org.tupol.spark.testing`






[PureConfig]: https://pureconfig.github.io/