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
package org.tupol.spark.io.streaming

import org.apache.spark.sql.streaming.DataStreamReader
import org.tupol.spark.io.sources.SourceConfiguration
import org.tupol.spark.io.{DataSource, DataSourceConfiguration, FormatAwareDataSinkConfiguration}

package object structured {

  implicit val StreamingSourceFactory =
    new StreamingSourceFactory {
      override def apply[C <: DataSourceConfiguration](configuration: C): DataSource[C, DataStreamReader] =
        configuration match {
          //TODO There must be a better way to use the type system without the type cast
          case c: FileStreamDataSourceConfiguration => FileStreamDataSource(c).asInstanceOf[DataSource[C, DataStreamReader]]
          case c: KafkaStreamDataSourceConfiguration => KafkaStreamDataSource(c).asInstanceOf[DataSource[C, DataStreamReader]]
          case c: GenericStreamDataSourceConfiguration => GenericStreamDataSource(c).asInstanceOf[DataSource[C, DataStreamReader]]
          case u => throw new IllegalArgumentException(s"Unsupported streaming configuration type ${u.getClass}.")
        }
    }

  trait StreamingSourceFactory {
    def apply[Config <: DataSourceConfiguration](configuration: Config): DataSource[Config, DataStreamReader]
  }

  trait StreamingConfiguration
  trait StreamingSourceConfiguration extends DataSourceConfiguration with StreamingConfiguration

  trait FormatAwareStreamingSourceConfiguration extends StreamingSourceConfiguration with SourceConfiguration

  /** Common marker trait for `DataSink` configuration that also knows the data format */
  trait FormatAwareStreamingSinkConfiguration extends FormatAwareDataSinkConfiguration with StreamingConfiguration

  /**
   * A Kafka subscription is defined by its type (e.g. subscribe or subscribe patterns) and the
   * subscription itself (e.g. the actual topic name that this subscription is defining).
   * @param subscriptionType the type of the subscription (e.g. assign, subscribe or subscribe patterns)
   * @param subscription the topic name to subscribe to
   */
  case class KafkaSubscription(subscriptionType: String, subscription: String)
  object KafkaSubscription{
    val AcceptableTypes = Seq("assign", "subscribe", "subscribePattern")
  }
}
