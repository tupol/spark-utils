# FileStreamDataSource


## Description

The `FileStreamDataSource` framework is a utility framework that helps configuring and reading `DataFrame`s from file streams.

This framework provides for reading from a given stream with the specified format like `avro`, `parquet`, `orc`, `json`,
`csv`...

The framework is composed of two classes:
- `FileStreamDataSource`, which is created based on a `FileStreamDataSourceConfiguration` class and provides one main function:
  ```scala 
  override def read(implicit spark: SparkSession): Try[DataFrame]
  ```
- `FileStreamDataSourceConfiguration`: the necessary configuration parameters

**Sample code**
```scala
import org.tupol.spark.io._

implicit val sparkSession: SparkSession = ???
val sourceConfiguration: FileStreamDataSourceConfiguration = ???
val dataframe = FileStreamDataSource(sourceConfiguration).read
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io.implicits._`.

**Sample code**
```scala
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.streaming.structured._

val sourceConfiguration: FileStreamDataSourceConfiguration = ???
val dataframe = spark.streamingSource(sourceConfiguration).read
```


## Configuration Parameters

### Common Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `path` **Required**
  - the input file(s) path
  - it can be a local file or an hdfs file or an hdfs compatible file (like s3 or wasb)
  - it accepts patterns like `hdfs://some/path/2018-01-*/hours=0*/*`
- `schema` **Required**
  - this is a parameter that represents the json Apache Spark schema that should be enforced on 
    the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file
  - `schema.path` *Optional*
    - this is an optional parameter that represents local path or the class path to the json 
      Apache Spark schema that should be enforced on the input data
    - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
    - if this parameter is found the schema will be loaded from the given file, otherwise, 
      the `schema` parameter is tried

### Format Specific Parameters

For more details check the [File Data Source](file-data-source.md#configuration-parameters)


## References

- [File Data Source](file-data-source.md#configuration-parameters)
