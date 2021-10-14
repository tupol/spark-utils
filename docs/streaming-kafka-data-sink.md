# KafkaStreamDataSink


## Description

The `KafkaStreamDataSink` framework is a utility framework that helps configuring and writing `DataFrame`s to streams.

The framework is composed of two classes:
- `KafkaStreamDataSink`, which is created based on a `KafkaStreamDataSinkConfiguration` class and provides one main function:
    ```scala
    def write(implicit spark: SparkSession): Try[StreamingQuery]
    ```
- `KafkaStreamDataSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = KafkaStreamDataSinkConfiguration(...)
    val dataframe = KafkaStreamDataSink(sourceConfiguration).write(data)
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    import org.tupol.spark.implicits._
    ...
    val sourceConfiguration = KafkaStreamDataSinkConfiguration(...)
    val dataframe = data.streamingSink(sourceConfiguration).write
```


## Configuration Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are:  `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `kafka.bootstrap.servers` **Required** 
- `topic` **Required** 
- `trigger` *Optional*
   - `type`: possible values: "Continuous", "Once", "ProcessingTime" 
   - `interval`: mandatory for "Continuous", "ProcessingTime" 
- `queryName` *Optional*
- `partition.columns` *Optional*
- `outputMode` *Optional*
- `checkpointLocation` *Optional*
  

## References

- [Structured Streaming Programming Guide - Output Sinks][SSOS]
- [Structured Streaming + Kafka Integration Guide][SSKIG]


[SSOS]: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
[SSKIG]: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
