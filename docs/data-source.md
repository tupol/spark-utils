# DataSource


## Description

The `DataSource` framework is a utility framework that helps configuring and reading `DataFrame`s.

This framework provides for reading from a given path with the specified format like `avro`, `parquet`, `orc`, `json`,
`csv`, `jdbc`...

The framework is composed of two main traits:
- `DataSource`, which is created based on a `FileSourceConfiguration` class and provides one main function:
    `def read(implicit spark: SparkSession): DataFrame`
- `DataSourceConfiguration`: a marker trait to define `DataSource` configuration classes


## Usage

The framework provides the following predefined `DataSource` implementations:

- [FileDataSource](file-data-source.md)
- [JdbcDataSource](jdbc-data-source.md)

For convenience the `DataSourceFactory` trait and the default implementation are provided.
To create a `DataSource` out of a given TypeSafe `Config` instance, one can call

```scala
DataSource( someDataSourceConfigurationInstance )
```

Also, in order to easily extract the configuration from a given TypeSafe `Config` instance,
the `FormatAwareDataSourceConfiguration` factory is provided.

```scala
FormatAwareDataSourceConfiguration( someTypesafeConfigurationInstance )
```


## Configuration Parameters

- [FileDataSource Configuration Parameters](file-data-source.md#configuration-parameters)
- [JdbcDataSource Configuration Parameters](jdbc-data-source.md#configuration-parameters)
