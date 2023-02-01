/*
MIT License

Copyright (c) 2018 Tupol (github.com/tupol)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package org.tupol.spark.io.streaming.structured


import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tupol.spark.Logging
import org.tupol.spark.io.FormatType._
import org.tupol.spark.io.{DataSource, FormatType}

import scala.util.Try

case class KafkaStreamDataSource(configuration: KafkaStreamDataSourceConfiguration) extends DataSource[KafkaStreamDataSourceConfiguration, DataStreamReader] with Logging {

  private val genericSource = GenericStreamDataSource(configuration.generic)

  /** Create a `DataFrameReader` using the given configuration and the `spark` session available. */
  override def reader(implicit spark: SparkSession): DataStreamReader = genericSource.reader

  /** Read a `DataFrame` using the given configuration and the `spark` session available. */
  override def read(implicit spark: SparkSession): Try[DataFrame] = genericSource.read

}

/**
 * Basic configuration for the `KafkaDataSource`
 */
case class KafkaStreamDataSourceConfiguration(
  kafkaBootstrapServers: String,
  subscription: KafkaSubscription,
  startingOffsets: Option[String] = None,
  endingOffsets: Option[String] = None,
  failOnDataLoss: Option[Boolean] = None,
  kafkaConsumerPollTimeoutMs: Option[Long] = None,
  fetchOffsetNumRetries: Option[Int] = None,
  fetchOffsetRetryIntervalMs: Option[Long] = None,
  maxOffsetsPerTrigger: Option[Long] = None,
  schema: Option[StructType] = None,
  options: Map[String, String] = Map())
  extends FormatAwareStreamingSourceConfiguration {
  /** Get the format type of the input file. */
  def format: FormatType = Kafka
  private val internalOptions =
    Map(
      "kafka.bootstrap.servers" -> Some(kafkaBootstrapServers),
      subscription.subscriptionType -> Some(subscription.subscription),
      "startingOffsets" -> startingOffsets,
      "endingOffsets" -> endingOffsets,
      "failOnDataLoss" -> failOnDataLoss,
      "kafkaConsumer.pollTimeoutMs" -> kafkaConsumerPollTimeoutMs,
      "fetchOffset.numRetries" -> fetchOffsetNumRetries,
      "fetchOffset.retryIntervalMs" -> fetchOffsetRetryIntervalMs,
      "maxOffsetsPerTrigger" -> maxOffsetsPerTrigger)
      .collect { case (key, Some(value)) => (key, value.toString) }
    val generic = GenericStreamDataSourceConfiguration(format, options ++ internalOptions, schema)
}


