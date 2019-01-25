# DataSink


## Description

The `DataSink` framework is a utility framework that helps configuring and writing `DataFrame`s.

This framework provides for reading from a given path with the specified format like `avro`, `parquet`, `orc`, `json`,
`csv`, `jdbc`...

The framework is composed of two main traits:
- `DataSink`, which is created based on a `FileSourceConfiguration` class and provides one main function:
    `def write(data: DataFrame): DataFrame`
- `DataSinkConfiguration`: a marker trait to define `DataSink` configuration classes


## Usage

The framework provides the following predefined `DataSink` implementations:

- [FileDataSink](file-data-sink.md)
- [JdbcDataSink](jdbc-data-sink.md)

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


## Configuration Parameters

- [FileDataSink Configuration Parameters](file-data-sink.md#configuration-parameters)
- [JdbcDataSink Configuration Parameters](jdbc-data-sink.md#configuration-parameters)
