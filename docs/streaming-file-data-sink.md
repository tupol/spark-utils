# FileStreamDataSink


## Description

The `FileStreamDataSink` framework is a utility framework that helps configuring and writing `DataFrame`s to streams.

The framework is composed of two classes:
- `FileStreamDataSink`, which is created based on a `FileStreamDataSinkConfiguration` class and provides one main function:
    ```scala
    def write(implicit spark: SparkSession): Try[StreamingQuery]
    ```
- `FileStreamDataSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = FileStreamDataSinkConfiguration(...)
    val dataframe = FileStreamDataSink(sourceConfiguration).write(data)
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    import org.tupol.spark.implicits._
    ...
    val sourceConfiguration = FileStreamDataSinkConfiguration(...)
    val dataframe = data.streamingSink(sourceConfiguration).write
```


## Configuration Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are:  `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `path` **Required**
- `trigger` *Optional*
   - `type`: possible values: "Continuous", "Once", "ProcessingTime" 
   - `interval`: mandatory for "Continuous", "ProcessingTime" 
- `queryName` *Optional*
- `partition.columns` *Optional*
- `outputMode` *Optional*
- `checkpointLocation` *Optional*
  
### Format Specific Parameters

For more details check the [File Data Sink](file-data-sink.md#configuration-parameters)


## References

- [File Data Sink](file-data-sink.md#configuration-parameters)
