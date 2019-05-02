# GenericStreamDataSource


## Description

The `GenericStreamDataSource` framework is a utility framework that helps configuring and reading `DataFrame`s from streams.

The framework is composed of two classes:
- `GenericStreamDataSource`, which is created based on a `GenericStreamDataSourceConfiguration` class and provides one main function:
    `def read(implicit spark: SparkSession): DataFrame`
- `GenericStreamDataSourceConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = GenericStreamDataSourceConfiguration(...)
    val dataframe = GenericStreamDataSource(sourceConfiguration).read
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    import org.tupol.spark.implicits._
    ...
    val sourceConfiguration = GenericStreamDataSourceConfiguration(...)
    val dataframe = spark.source(sourceConfiguration).read
```


## Configuration Parameters

### Common Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are: 
    - `socket`
    - `kafka`
    - file sources: `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
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
  - the schema is applied on read only on file streams; for other streams the developer has to 
  find a way to apply it.

### File Parameters

- `path` **Required**
-  For more details check the [File Data Source](file-data-source.md#configuration-parameters)
   
### Socket Parameters

**Warning: Not for production use!**

- `options` **Required**
  - `host` **Required**
  - `port` **Required**
  - `includeTimestamp` *Optional* 
   
  
### Kafka Parameters

- `options` **Required**
  - `kafka.bootstrap.servers` **Required** 
  - `assign` | `subscribe` | `subscribePattern` **Required** * 
  - `startingOffsets` *Optional* 
  - `endingOffsets` *Optional* 
  - `failOnDataLoss` *Optional* 
  - `kafkaConsumer.pollTimeoutMs` *Optional* 
  - `fetchOffset.numRetries` *Optional* 
  - `fetchOffset.retryIntervalMs` *Optional* 
  - `maxOffsetsPerTrigger` *Optional* 


## References

- [File Data Source](file-data-source.md#configuration-parameters)
- [Structured Streaming Programming Guide - Input Sources][SSIS]
- [Structured Streaming + Kafka Integration Guide][SSKIG]


[SSIS]: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources
[SSKIG]: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
