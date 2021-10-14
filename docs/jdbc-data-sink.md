# JdbcDataSink


## Description

The `JdbcDataSink` framework is a utility framework that helps configuring and writing `DataFrames`.

This framework provides for writing to a given Jdbc connection.

This framework supports different save modes like `overwrite` or `append`, as well as partitioning parameters like
columns and number of partition files.

The framework is composed of two classes:
- `JdbcDataSink`, which is created based on a `JdbcSinkConfiguration` class and provides one main function:
    ```scala
    def write(data: DataFrame): Try[DataFrame]
    ```
- `JdbcSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = JdbcSinkConfiguration(outputPath, format)
    JdbcDataSink(sinkConfiguration).write(dataframe)
```

Optionally, one can use the implicit decorator for the `DataFrame` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = JdbcSinkConfiguration(outputPath, format)
    dataframe.sink(sinkConfiguration).write
```


## Configuration Parameters

- `url` **Required**
  - the JDBC friendly URL pointing to the source data base
- `table` **Required**
  - the source table
- `user` *Optional*
  - the data base connection user
- `password` *Optional*
  - the data base connection password
- `driver` *Optional*
  - the JDBc driver class
- `schema` *Optional*
  - this is an optional parameter that represents the json Apache Spark schema that should 
    be enforced on the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file
- `options` *Optional*
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file
  - for more details about the available options please check the [References](#references) section.
- `mode` *Optional*
    - the save mode can be `overwrite`, `append`, `ignore` and `error`;
    - more details available [here](https://spark.apache.org/docs/2.3.1/api/scala/#org.apache.spark.sql.DataFrameWriter)
- `options` *Optional*
  - additional options that can be passed to the Apache Spark `DataFrameWriter`;
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file
  - more details available [here](https://spark.apache.org/docs/2.3.1/api/scala/#org.apache.spark.sql.DataFrameWriter)


## References

For the more details about the optional parameters consult the
[DataFrameWriter](https://spark.apache.org/docs/2.3.2/api/scala/index.html?org/apache/spark/sql/package-tree.html#org.apache.spark.sql.DataFrameWriter)
API and sources, especially
[JDBCOptions](https://github.com/apache/spark/2.3.2/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCOptions.scala).
