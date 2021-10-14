# GenericDataSink


## Description

The `GenericDataSink` framework is a utility framework that helps configuring and writing 
`DataFrames`.

This framework provides for writing to a given path into the specified format like 
[`delta`](https://github.com/delta-io/delta). 
Keep in mind that custom data sources usually require custom libraries dependencies or 
custom packages dependencies.

This framework supports different save modes like `overwrite` or `append`, as well as 
partitioning parameters like columns.

The framework is composed of two classes:
- `GenericDataSink`, which is created based on a `GenericSinkConfiguration` class and provides one main function:
    ```scala
    def write(data: DataFrame): Try[DataFrame]
    ```
- `GenericSinkConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = GenericSinkConfiguration(format, optionalSaveMode, 
                                      partitionColumns, optionalBuckets, options)
    GenericDataSink(sinkConfiguration).write(dataframe)
```

Optionally, one can use the implicit decorator for the `DataFrame` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = GenericSinkConfiguration(format, optionalSaveMode, 
                                      partitionColumns, optionalBuckets, options)
    dataframe.sink(sinkConfiguration).write
```


## Configuration Parameters

- `format` **Required**
  - the format of the output
  - any value is acceptable, but it needs to be supported by Spark
- `mode` *Optional*
  - the save mode can be `overwrite`, `append`, `ignore` and `error`; more details available
  [here](https://spark.apache.org/docs/2.3.1/api/scala/#org.apache.spark.sql.DataFrameWriter);
- `partition.columns` *Optional*
  - a sequence of columns that should be used for partitioning data on disk;
  - they should exist in the result of the given sql;
  - if empty no partitioning will be performed.
- `buckets` *Optional*
  - define if the output will be bucketed "Hive" style
  - if any of these parameters fail during configuration validation the entire bucketing 
    configuration will be ignored
  - the used output function is `saveAsTable` using the `path` parameter as the table name
  - `number` **Required** 
    - the number of buckets
  - `bucketColumns` **Required** 
    - columns used for bucketing
  - `sortByColumns` *Optional*
    - sort columns
- `options` *Optional*
  - additional options that can be passed to the Apache Spark `DataFrameWriter`;
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file;
  - some useful options, available for *most* of the output formats are:
    - `compression`:  possible values like `none`, `bzip2`, `gzip`, `lz4`, `snappy`
    - `dateFormat`: default `yyyy-MM-dd`
    - `timestampFormat`: default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`
  - more details available [here](https://spark.apache.org/docs/2.3.1/api/scala/#org.apache.spark.sql.DataFrameWriter);


## References

For the more details about the optional parameters consult the
[DataFrameWriter](https://spark.apache.org/docs/2.3.2/api/scala/index.html?org/apache/spark/sql/package-tree.html#org.apache.spark.sql.DataFrameWriter)
API and sources.
