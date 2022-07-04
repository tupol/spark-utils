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

import org.tupol.spark.io.{ DataSourceConfiguration, FormatAware, FormatAwareDataSinkConfiguration }

package object structured {

  trait StreamingConfiguration
  trait StreamingSourceConfiguration extends DataSourceConfiguration with StreamingConfiguration

  trait FormatAwareStreamingSourceConfiguration extends StreamingSourceConfiguration with FormatAware

  /** Common marker trait for `DataSink` configuration that also knows the data format */
  trait FormatAwareStreamingSinkConfiguration extends FormatAwareDataSinkConfiguration with StreamingConfiguration

  /**
   * A Kafka subscription is defined by it's type (e.g. subscribe or subscribe patterns) and the
   * subscription itself (e.g. the actual topic name that this subscription is defining).
   * @param subscriptionType the type of the subscription (e.g. assign, subscribe or subscribe patterns)
   * @param subscription the topic name to subscribe to
   */
  case class KafkaSubscription(subscriptionType: String, subscription: String)
  object KafkaSubscription{
    val AcceptableTypes = Seq("assign", "subscribe", "subscribePattern")
  }
}
