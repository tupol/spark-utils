# KafkaStreamDataSink


## Description

The `KafkaStreamDataSink` framework is a utility framework that helps configuring and writing `DataFrame`s to streams.

The framework is composed of two classes:
- `KafkaStreamDataSink`, which is created based on a `KafkaStreamDataSinkConfiguration` class and provides two main functions:
    ```scala
    def writer(data: DataFrame): Try[DataStreamWriter[Row]]
    def write(implicit spark: SparkSession): Try[StreamingQuery]
    ```
- `KafkaStreamDataSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
import org.tupol.spark.io._

implicit val sparkSession: SparkSession = ???
val sourceConfiguration: KafkaStreamDataSinkConfiguration = ???
val dataframe = KafkaStreamDataSink(sourceConfiguration).write(data)
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io.implicits._`.

**Sample code**
```scala
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.streaming.structured._

val sourceConfiguration: KafkaStreamDataSinkConfiguration = ???
val dataframe = data.streamingSink(sourceConfiguration).write
```


## Configuration Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are:  `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `kafkaBootstrapServers` **Required** 
- `topic` **Required** 
- `trigger` *Optional*
   - `type`: possible values: "continuous", "once", "available-now", "processing-time"
   - `interval`: mandatory for "continuous", "processing-time" 
- `queryName` *Optional*
- `partition.columns` *Optional*
- `outputMode` *Optional*
- `checkpointLocation` *Optional*
  

## References

- [Structured Streaming Programming Guide - Output Sinks][SSOS]
- [Structured Streaming + Kafka Integration Guide][SSKIG]


[SSOS]: https://spark.apache.org/docs/3.0.1/structured-streaming-programming-guide.html#output-sinks
[SSKIG]: https://spark.apache.org/docs/3.0.1/structured-streaming-kafka-integration.html
