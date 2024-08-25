# GenericStreamDataSink


## Description

The `GenericStreamDataSink` framework is a utility framework that helps configuring and writing `DataFrame`s to streams.

The framework is composed of two classes:
- `GenericStreamDataSink`, which is created based on a `GenericStreamDataSinkConfiguration` class and provides two main functions:
    ```scala
    def writer(data: DataFrame): Try[DataStreamWriter[Row]]
    def write(implicit spark: SparkSession): Try[StreamingQuery]
    ```
- `GenericStreamDataSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
import org.tupol.spark.io._

implicit val sparkSession: SparkSession = ???
val sourceConfiguration: GenericStreamDataSinkConfiguration = ???
val dataframe = GenericStreamDataSink(sourceConfiguration).write(data)
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io.implicits._`.

**Sample code**
```scala
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._

val sourceConfiguration: GenericStreamDataSinkConfiguration = ???
val dataframe = data.streamingSink(sourceConfiguration).write
```


## Configuration Parameters

### Common Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are: 
    - `kafka`
    - file sources: `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `trigger` *Optional*
   - `type`: possible values: "continuous", "once", "available-now", "processing-time"
   - `interval`: mandatory for "continuous", "processing-time" 
- `queryName` *Optional*
- `partition.columns` *Optional*
- `outputMode` *Optional*
- `checkpointLocation` *Optional*

### File Parameters

- `options` **Required**
  - `path` **Required**
  -  For more details check the [File Data Sink](file-data-sink.md#configuration-parameters)
  
### Kafka Parameters

- `options` **Required**
  - `kafkaBootstrapServers` **Required** 
  - `topic` **Required** 


## References

- [File Data Sink](file-data-sink.md#configuration-parameters)
- [Structured Streaming Programming Guide - Output Sinks][SSOS]
- [Structured Streaming + Kafka Integration Guide][SSKIG]


[SSOS]: https://spark.apache.org/docs/3.0.1/structured-streaming-programming-guide.html#output-sinks
[SSKIG]: https://spark.apache.org/docs/3.0.1/structured-streaming-kafka-integration.html
