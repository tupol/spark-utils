# FileDataSource


## Description

The `FileDataSource` framework is a utility framework that helps configuring and reading `DataFrame`s.

This framework provides for reading from a given path with the specified format like `avro`, `parquet`, `orc`, `json`,
`csv`...

The framework is composed of two classes:
- `FileDataSource`, which is created based on a `FileSourceConfiguration` class and provides one main function:
    `def read(implicit spark: SparkSession): DataFrame`
- `FileSourceConfiguration`: the necessary configuration parameters

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    implicit val sparkSession = ...
    val sourceConfiguration = FileSourceConfiguration(inputPath, parserConfig)
    val dataframe = FileDataSource(inputConfig).read
```

Optionally, one can use the implicit decorator for the `SparkSession` available by importing `org.tupol.spark.io._`.

**Sample code**
```scala
    import org.tupol.spark.io._
    ...
    val sourceConfiguration = FileSourceConfiguration(inputPath, parserConfig)
    val dataframe = spark.source(sourceConfiguration).read
```


## Configuration Parameters

### Common Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are: `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `path` **Required**
  - the input file(s) path
  - it can be a local file or an hdfs file or an hdfs compatible file (like s3 or wasb)
  - it accepts patterns like `hdfs://some/path/2018-01-*/hours=0*/*`
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

### CSV Parameters

- `header` *Optional*
  - header defining if the reader should take the first row of the csv file as the column names
  - all types will be assumed string
  - the default value is `false`
  - if there is a `header` property also defined in the `options` section, this one will override it
- `delimiter` *Optional*
  - delimiter defining which separator has to be taken into account to distinct the data in the csv file
  - the default value is `,`
  - if there is a `delimiter` property also defined in the `options` section, this one will override it
- `options` *Optional*
  - due to it's complex structure, this parameter can not be passed as a command line argument, but it can only be
    passed through the `application.conf` file
  - the csv parser options are defined [here](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html#csv(java.lang.String...)).
    - `sep` (default `,`): sets the single character as a separator for each field and value.
    - `encoding` (default `UTF-8`): decodes the CSV files by the given encoding type.
    - `quote` (default `"`): sets the single character used for escaping quoted values where the separator can be part
      of the value. If you would like to turn off quotations, you need to set not null but an empty string.
      This behaviour is different from com.databricks.spark.csv.
    - `escape` (default `\`): sets the single character used for escaping quotes inside an already quoted value.
    - `comment` (default _empty string_): sets the single character used for skipping lines beginning with this character.
      By default, it is disabled.
    - `header` (default `false`): uses the first line as names of columns.
    - `inferSchema` (default `false`): infers the input schema automatically from data. It requires one extra pass over
      the data.
    - `ignoreLeadingWhiteSpace` (default `false`): defines whether or not leading whitespaces from values being read
      should be skipped.
    - `ignoreTrailingWhiteSpace` (default `false`): defines whether or not trailing whitespaces from values being read
      should be skipped.
    - `nullValue` (default _empty string_): sets the string representation of a null value. Since 2.0.1, this applies
      to all supported types including the string type.
    - `nanValue` (default `NaN`): sets the string representation of a non-number" value.
    - `positiveInf` (default `Inf`): sets the string representation of a positive infinity value.
    - `negativeInf` (default `-Inf`): sets the string representation of a negative infinity value.
    - `dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date code. Custom date formats follow the
      formats at `java.text.SimpleDateFormat`. This applies to date type.
    - `timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that indicates a timestamp code.
      Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
    - `maxColumns` (default `20480`): defines a hard limit of how many columns a record can have.
    - `maxCharsPerColumn` (default `-1`): defines the maximum number of characters allowed for any given value being read.
      By default, it is -1 meaning unlimited length
    - `maxMalformedLogPerPartition` (default `10`): sets the maximum number of malformed rows Spark will log for each
       partition. Malformed records beyond this number will be ignored.
    - `mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records during parsing.
      - `PERMISSIVE` : sets other fields to null when it meets a corrupted record. When a schema is set by user, it
         sets null for extra fields.
      - `DROPMALFORMED` : ignores the whole corrupted records.
      - `FAILFAST` : throws an exception when it meets corrupted records.
  
### XML Parameters

- `rowTag` **Required**
  - the XML tag name that will become rows of data
  - if the row tag is the root tag the output will be just one record
  - if there is a `rowTag` property also defined in the `options` section, this one will override it
- `options` *Optional*
  - due to it's complex structure, this parameter can not be passed as a command line argument, but it can only be
    passed through the `application.conf` file
  - Specific Databricks XML parser options; more details are available [here](https://github.com/databricks/spark-xml)
    - `path`: Location of files. Similar to Spark can accept standard Hadoop globbing expressions.
    - `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`,
      the appropriate value would be `book`. Default is `ROW`. At the moment, rows containing self closing xml tags
      are not supported. If the `rowTag` parameter is defined here, it will be overridden by the global `rowTag` parameter.
    - `samplingRatio`: Sampling ratio for inferring schema (0.0 ~ 1). Default is 1. Possible types are `StructType`,
      `ArrayType`, `StringType`, `LongType`, `DoubleType`, `BooleanType`, `TimestampType` and `NullType`, unless user
      provides a schema for this.
    - `excludeAttribute` : Whether you want to exclude attributes in elements or not. Default is false.
    - `treatEmptyValuesAsNulls` : (DEPRECATED: use `nullValue` set to `""`) Whether you want to treat whitespaces as
      a null value. Default is false
    - `attributePrefix`: The prefix for attributes so that we can differentiate attributes and elements.
      This will be the prefix for field names. Default is `_`.
    - `valueTag`: The tag used for the value when there are attributes in the element having no child.
      Default is `_VALUE`.
    - `charset`: Defaults to `UTF-8` but can be set to other valid charset names
    - `ignoreSurroundingSpaces`: Defines whether or not surrounding whitespaces from values being read should be skipped.
      Default is `false`.
    - `columnNameOfCorruptRecord` (default _none_): If defined a column with this name will be attached to the input
      data set schema and it will only be filled with corrupt records.
    - `mode`: The mode for dealing with corrupt records during parsing. Default is `PERMISSIVE`.
      - `PERMISSIVE` : sets other fields to `null` when it meets a corrupted record, and puts the malformed string
        into a new field configured by `columnNameOfCorruptRecord`. When a schema is set by user,
        it sets `null` for extra fields.
      - `DROPMALFORMED` : ignores the whole corrupted records.
      - `FAILFAST` : throws an exception when it meets corrupted records.

### JSON Parameters

- `options` *Optional*
  - due to it's complex structure, this parameter can not be passed as a command line argument, but it can only be
    passed through the `application.conf` file
  - the json parser options are defined [here](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html#json(java.lang.String...)).
    - `primitivesAsString` (default `false`): infers all primitive values as a string type
    - `prefersDecimal` (default `false`): infers all floating-point values as a decimal type. If the values do not fit
      in decimal, then it infers them as doubles.
    - `allowComments` (default `false`): ignores Java/C++ style comment in JSON records
    - `allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names
    - `allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
    - `allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers (e.g. 00012)
    - `allowBackslashEscapingAnyCharacter` (default `false`): allows accepting quoting of all character using backslash
      quoting mechanism.
    - `dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date code. Custom date formats follow the
      formats at java.text.SimpleDateFormat. This applies to date type.
    - `timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that indicates a timestamp code.
      Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to timestamp type.
    - `columnNameOfCorruptRecord` (default _none_): If defined a column with this name will be attached to the input
      data set schema and it will only be filled with corrupt records.
    - `mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records during parsing.
      - `PERMISSIVE` : sets other fields to null when it meets a corrupted record, and puts the malformed string into
        a new field configured by `columnNameOfCorruptRecord`. When a schema is set by user, it sets null for extra fields.
      - `DROPMALFORMED` : ignores the whole corrupted records.
      - `FAILFAST` : throws an exception when it meets corrupted records.


## References

- For the CSV, JSON, Parquet and Text converter API see more details
[here](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html).
- For the XML converter API see more details [here](https://github.com/databricks/spark-xml).
- For the Avro converter API see more details [here](https://github.com/databricks/spark-avro).
