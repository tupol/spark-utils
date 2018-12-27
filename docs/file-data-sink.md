# FileDataSink

## Description

The `FileDataSink` framework is a utility framework that helps configuring and writing `DataFrames`.

This framework provides for writing to a given path into the specified format like `avro`, `parquet`, `orc`, `json`, `csv`...

This framework supports different save modes like `overwrite` or `append`, as well as partitioning parameters like
columns and number of partition files.

The framework is composed of two classes:
- `FileDataSink`, which is created based on a `FileSinkConfiguration` class and provides one main function:
    `def write(data: DataFrame): Try[DataFrame]`
- `FileSinkConfiguration`: the necessary configuration parameters

**Sample code**
```
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = FileSinkConfiguration(outputPath, format)
    FileDataSink(sinkConfiguration).write(dataframe)
```

Optionally, one can use the implicit decorator for the `DataFrame` available by importing `org.tupol.spark.io._`.

**Sample code**
```
    import org.tupol.spark.io._
    ...
    val sinkConfiguration = FileSinkConfiguration(outputPath, format)
    dataframe.sink(sinkConfiguration).write
```


## Configuration Parameters

- `path`: the path where the result will be saved
- `format`: the format of the output file; acceptable values are `json`, `avro`, `orc` and `parquet`
- `mode`: the save mode can be `overwrite`, `append`, `ignore` and `error`; more details available
    [here](https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/DataFrameWriter.html#mode-java.lang.String-)
- `partition.columns`: a sequence of columns that should be used for partitioning data on disk;
    they should exist in the result of the given sql;
    if empty no partitioning will be performed.
- `partition.files`: the number of partition files that will end up in each partition folder; one can always
    look at the average size of the data inside partition folders and come up with a number that is appropriate for the
        application;  for example, one might target a partition number so that the partition file size inside a
        partition folder are around 100 MB
- `options` *Optional*: additional options that can be passed to the Apache Spark `DataFrameWriter`;
    due to it's complex structure, this parameter can not be passed as a command line argument, but it can only be
        passed through the `application.conf` file
