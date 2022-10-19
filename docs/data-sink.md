# DataSink


## Description

The `DataSink` framework is a utility framework that helps configuring and writing `DataFrame`s.

This framework provides for reading from a given path with the specified format like `avro`, `parquet`, `orc`, `json`,
`csv`, `jdbc`...

The framework is composed of two main traits:
- `DataSink`, which is created based on a `FileSourceConfiguration` class and provides two main functions:
    ```scala
    def writer(data: DataFrame): Try[DataFrameWriter[Row]]
    def write(data: DataFrame): Try[DataFrame]
    ```
- `DataSinkConfiguration`: a marker trait to define `DataSink` configuration classes


## Usage

The framework provides the following predefined `DataSink` implementations:

- [FileDataSink](file-data-sink.md)
- [JdbcDataSink](jdbc-data-sink.md)
- [GenericDataSink](generic-data-sink.md)
- [FileStreamDataSink](streaming-file-data-sink.md)
- [KafkaStreamDataSink](streaming-kafka-data-sink.md)
- [GenericStreamDataSink](streaming-generic-data-sink.md)

For convenience the `DataAwareSinkFactory` trait and the default implementation are provided.
To create a `DataSink` out of a given TypeSafe `Config` instance, one can call

```scala
DataAwareSinkFactory( someDataSinkConfigurationInstance )
```

Also, in order to easily extract the configuration from a given TypeSafe `Config` instance,
the `FormatAwareDataSinkConfiguration` factory is provided.

```scala
FormatAwareDataSinkConfiguration( someTypesafeConfigurationInstance )
```

There is a convenience implicit decorator for DataFrames, available through the
```scala
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._
```
import statements.
The `org.tupol.spark.io` package contains the implicit factories for data sinks and the `org.tupol.spark.implicits`
contains the actual `DataFrame` decorator.

This allows us to create the `DataSink` by calling the `sink()` function on a DataFrame,
passing a `DataSinkConfiguration`  configuration instance.

```scala
import org.tupol.spark.io.{pureconf, _}
import org.tupol.spark.io.implicits._

def dataFrame: DataFrame = ???
def dataSinkConfiguration: DataSinkConfiguration = ???
val dataSink: DataSink[_, _, _] = dataFrame.sink(dataSinkConfiguration)
```


## Configuration Parameters

- [`FileDataSink` Configuration Parameters](file-data-sink.md#configuration-parameters)
- [`JdbcDataSink` Configuration Parameters](jdbc-data-sink.md#configuration-parameters)
- [`FileStreamDataSink` Configuration Parameters](streaming-file-data-sink.md#configuration-parameters)
- [`KafkaStreamDataSink` Configuration Parameters](streaming-kafka-data-sink.md#configuration-parameters)
- [`GenericStreamDataSink` Configuration Parameters](streaming-generic-data-sink.md#configuration-parameters)
