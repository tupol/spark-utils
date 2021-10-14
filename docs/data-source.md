# DataSource


## Description

The `DataSource` framework is a utility framework that helps configuring and reading `DataFrame`s.

This framework provides for reading from a given path with the specified format like 
`avro`, `parquet`, `orc`, `json`, `csv`, `jdbc`...

The framework is composed of two main traits:
- `DataSource`, which is created based on a `DataSourceConfiguration` class and provides one main function:
  ```scala 
  override def read(implicit spark: SparkSession): Try[DataFrame]
  ```
- `DataSourceConfiguration`: a marker trait to define `DataSource` configuration classes


## Usage

The framework provides the following predefined `DataSource` implementations:

- [FileDataSource](file-data-source.md)
- [JdbcDataSource](jdbc-data-source.md)
- [GenericDataSource](generic-data-source.md)
- [FileStreamDataSource](streaming-file-data-source.md)
- [KafkaStreamDataSource](streaming-kafka-data-source.md)
- [GenericStreamDataSource](streaming-generic-data-source.md)

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

There is a convenience implicit decorator for the Spark session, available through the
```scala
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._
```
import statements.
The `org.tupol.spark.io` package contains the implicit factories for data sources and the `org.tupol.spark.implicits`
contains the actual `SparkSession` decorator.

This allows us to create the `DataSource` by calling the `source()` function on the given Spark session,
passing a `DataSourceConfiguration`  configuration instance.

```scala
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._

def spark: SparkSession = ???
def dataSourceConfiguration: DataSourceConfiguration = ???
val dataSource: DataSource = spark.source(dataSourceConfiguration)
```


## Configuration Parameters

- [`FileDataSource` Configuration Parameters](file-data-source.md#configuration-parameters)
- [`JdbcDataSource` Configuration Parameters](jdbc-data-source.md#configuration-parameters)
- [`FileStreamDataSource` Configuration Parameters](streaming-file-data-source.md#configuration-parameters)
- [`KafkaStreamDataSource` Configuration Parameters](streaming-kafka-data-source.md#configuration-parameters)
- [`GenericStreamDataSource` Configuration Parameters](streaming-generic-data-source.md#configuration-parameters)
