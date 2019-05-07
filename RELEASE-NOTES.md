# Release Notes


## 0.4

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
