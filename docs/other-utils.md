# Various Core Utilities

[[__TOC__]]

## `org.tupol.spark.utils`

#### `fuzzyLoadTextResourceFile`
Try to load the contents of a text file into a string, by looking into the local file system, url, uri and finally classpath.
This can be useful when unsure where the file might be.

#### `JdbcSinkConfiguration`
Renamed `JdbcSinkConfiguration.optionalSaveMode` to `JdbcSinkConfiguration.mode` and added a utility method to 
provide some backward compatibility: `def optionalSaveMode: Option[String] = mode`

## `org.tupol.spark.implicits`

### Dataset Implicits

#### `def withColumnDataset[U: Encoder](column: Column): Dataset[(T, U)]`

```scala
import org.tupol.spark.implicits._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Dataset

val dataset: Dataset[MyClass] = ???
val datasetWithCol: Dataset[(MyClass, String)] = dataset.withColumnDataset[String](lit("some text"))
```

#### `def mapValues[U: Encoder](f: V => U): Dataset[(K, U)]`

```scala
import org.tupol.spark.implicits._
import org.apache.spark.sql.Dataset

val dataset: Dataset[(String, Int)] = ???
val result: Dataset[(String, Int)]  = dataset.mapValues(_ * 10)
```

#### `def flatMapValues[U: Encoder](f: V => TraversableOnce[U]): Dataset[(K, U)]`

```scala
import org.tupol.spark.implicits._
import org.apache.spark.sql.Dataset

val dataset: Dataset[(String, Int)] = ???
val result: Dataset[(String, Int)]  = dataset.flatMapValues(Seq(1, 2, 3))
```

## `org.tupol.spark.testing`

### `SparkSession` for testing

- `SharedSparkSession`: Shares a SparkSession, with the SparkContext and the SqlContext for all the tests in the Suite.
- `LocalSparkSession`: A fresh SparkSession, with the SparkContext and the SqlContext for each test in the Suite.

### `compareDataFrames`

This is a very useful testing method to compare data frames.
It is a fairly naive comparison function for two data frames using the join columns.
The tests are superficial and do not go deep enough, but they will do for now.
In order to see the actual test results one needs to examine the output `DataFrameCompareResult`, which can give a
binary answer through `areEqual` or it can print a comprehensive report through `show`.

```scala
/**
 * Naive comparison function for two data frames using the join columns.
 * The tests are superficial and do not go deep enough, but they will do for now.
 * In order to see the actual test results one needs to examine the output, [[DataFrameCompareResult]]
 * @param left `DataFrame`
 * @param right `DataFrame`
 * @param joinColumns the column names used to join the `left` and right` `DataFrame`s
 * @return `DataFrameCompareResult`
 */
def compareDataFrames(left: DataFrame, right: DataFrame, joinColumns: Seq[String] = Seq()): DataFrameCompareResult
```

Of course, there is an implicit decorator provided for `DataFrame`s, which allows to compare them easily like

```scala
import org.tupol.spark.testing._
...
val thisDataFrame: DataFrame = ???
val thatDataFrame: DataFrame = ???
...
thisDataFrame.compareWith(thatDataFrame, Seq("common_id_column")).areEqual
```