# JdbcDataSource


## Description

The `JdbcDataSource` framework is a utility framework that helps configuring and reading `DataFrame`s.

This framework provides for reading from a `jdbc` connection.

The framework is composed of two classes:
- `JdbcDataSource`, which is created based on a `JdbcDataSourceConfig` class and provides one main function:
    `def read(implicit spark: SparkSession): Try[DataFrame]`
- `JdbcDataSourceConfig`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = JdbcDataSourceConfig(inputPath, parserConfig)
    val dataframe = JdbcDataSource(inputConfig).read
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sourceConfiguration = JdbcDataSourceConfig(inputPath, parserConfig)
    val dataframe = spark.source(sourceConfiguration).read
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
- `schema.path` *Optional*
  - this is an optional parameter that represents local path or the class path to the json Apache Spark schema that
    should be enforced on the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - if this parameter is found the schema will be loaded from the given file, otherwise, the `schema` parameter is tried
- `schema` *Optional*
  - this is an optional parameter that represents the json Apache Spark schema that should be enforced on the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - due to it's complex structure, this parameter can not be passed as a command line argument, but it can only be
    passed through the `application.conf` file
- `options` *Optional*
  - due to it's complex structure, this parameter can not be passed as a command line argument, but it can only be
    passed through the `application.conf` file
  - for more details about the available options please check the [References](#references) section.


## References

- For the more details about the optional parameters consult the [DataFrameReader](https://spark.apache.org/docs/2.3.0/api/scala/index.html?org/apache/spark/sql/package-tree.html#org.apache.spark.sql.DataFrameReader)
  API and sources, especially [JDBCOptions](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCOptions.scala).
