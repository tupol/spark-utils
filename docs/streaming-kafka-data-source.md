# KafkaStreamDataSource


## Description

The `KafkaStreamDataSource` framework is a utility framework that helps configuring and reading `DataFrame`s from Kafka streams.

The framework is composed of two classes:
- `KafkaStreamDataSource`, which is created based on a `KafkaStreamDataSourceConfiguration` 
  class and provides one main function:
  ```scala 
  override def read(implicit spark: SparkSession): Try[DataFrame]
  ```
- `KafkaStreamDataSourceConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = KafkaStreamDataSourceConfiguration(...)
    val dataframe = KafkaStreamDataSource(sourceConfiguration).read
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    import org.tupol.spark.implicits._
    ...
    val sourceConfiguration = KafkaStreamDataSourceConfiguration(...)
    val dataframe = spark.source(sourceConfiguration).read
```


## Configuration Parameters

- `format` **Required**
  - acceptable value: `kafka`
- `kafka.bootstrap.servers` **Required**
- `subscription` **Required**
  - `type`: possible values are "assign", "subscribe", "subscribePattern"
  - `value`: depending on the type; for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]
- `startingOffsets` *Optional*
  - for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]
- `endingOffsets` *Optional*
  - for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]
- `failOnDataLoss` *Optional*
  - for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]
- `kafkaConsumer.pollTimeoutMs` *Optional*
  - for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]
- `fetchOffset.numRetries` *Optional*
  - for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]
- `fetchOffset.retryIntervalMs` *Optional*
  - for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]
- `maxOffsetsPerTrigger` *Optional*
  - for more details please check [Structured Streaming + Kafka Integration Guide][SSKIG]

- `schema` *Optional*
  - this is an optional parameter that represents the json Apache Spark schema that should be 
    enforced on the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - due to it's complex structure, this parameter can not be passed as a command line argument,
    but it can only be passed through the `application.conf` file
  - the schema is applied on read only on file streams; for other streams the developer has to 
    find a way to apply it. 
  - `schema.path` *Optional*
    - this is an optional parameter that represents local path or the class path to the json 
      Apache Spark schema that should be enforced on the input data
    - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
    - if this parameter is found the schema will be loaded from the given file, otherwise, 
      the `schema` parameter is tried



## References

- [Structured Streaming Programming Guide - Input Sources][SSIS]
- [Structured Streaming + Kafka Integration Guide][SSKIG]


[SSIS]: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources
[SSKIG]: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
