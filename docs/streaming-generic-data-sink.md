# GenericStreamDataSink


## Description

The `GenericStreamDataSink` framework is a utility framework that helps configuring and writing `DataFrame`s to streams.

The framework is composed of two classes:
- `GenericStreamDataSink`, which is created based on a `GenericStreamDataSinkConfiguration` class and provides one main function:
    ```scala
    def write(implicit spark: SparkSession): Try[StreamingQuery]
    ```
- `GenericStreamDataSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = GenericStreamDataSinkConfiguration(...)
    val dataframe = GenericStreamDataSink(sourceConfiguration).write(data)
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    import org.tupol.spark.io.implicits._
    ...
    val sourceConfiguration = GenericStreamDataSinkConfiguration(...)
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
   - `type`: possible values: "Continuous", "Once", "ProcessingTime" 
   - `interval`: mandatory for "Continuous", "ProcessingTime" 
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
  - `kafka.bootstrap.servers` **Required** 
  - `topic` **Required** 


## References

- [File Data Sink](file-data-sink.md#configuration-parameters)
- [Structured Streaming Programming Guide - Output Sinks][SSOS]
- [Structured Streaming + Kafka Integration Guide][SSKIG]


[SSOS]: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
[SSKIG]: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
