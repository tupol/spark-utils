# GenericDataSource


## Description

The `GenericDataSource` framework is a utility framework that helps configuring and 
reading `DataFrame`s.

This framework provides for reading from a custom data source, like 
[`delta`](https://github.com/delta-io/delta). 
Keep in mind that custom data sources usually require custom libraries dependencies or 
custom packages dependencies.

The framework is composed of two classes:
- `GenericDataSource`, which is created based on a `GenericSourceConfiguration` class and provides one main function:
  ```scala 
  def read(implicit spark: SparkSession): DataFrame
  ```
- `GenericSourceConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = GenericSourceConfiguration(format, options, schema)
    val dataframe = GenericDataSource(sourceConfiguration).read
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    import org.tupol.spark.implicits._
    ...
    val sourceConfiguration = GenericSourceConfiguration(format, options, schema)
    val dataframe = spark.source(sourceConfiguration).read
```


## Configuration Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - any value is acceptable, but it needs to be supported by Spark
- `schema.path` *Optional*
  - this is an optional parameter that represents local path or the class path to the json Apache 
    Spark schema that should be enforced on the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - if this parameter is found the schema will be loaded from the given file, otherwise, the `schema` parameter is tried
- `schema` *Optional*
  - this is an optional parameter that represents the json Apache Spark schema that should be 
    enforced on the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file
- `options` *Optional*
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file
