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

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger
import org.tupol.spark.io.{ DataSourceConfiguration, FormatAware, FormatAwareDataSinkConfiguration }
import org.tupol.utils.configz._
import scalaz.ValidationNel

package object structured {

  trait StreamingConfiguration
  trait StreamingSourceConfiguration extends DataSourceConfiguration with StreamingConfiguration

  trait FormatAwareStreamingSourceConfiguration extends StreamingSourceConfiguration with FormatAware
  object FormatAwareStreamingSourceConfiguration extends Configurator[FormatAwareStreamingSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, FormatAwareStreamingSourceConfiguration] =
      config.extract[FileStreamDataSourceConfiguration] orElse
        config.extract[KafkaStreamDataSourceConfiguration] orElse
        config.extract[GenericStreamDataSourceConfiguration]
  }

  /** Common marker trait for `DataSink` configuration that also knows the data format */
  trait FormatAwareStreamingSinkConfiguration extends FormatAwareDataSinkConfiguration with StreamingConfiguration
  object FormatAwareStreamingSinkConfiguration extends Configurator[FormatAwareStreamingSinkConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, FormatAwareStreamingSinkConfiguration] =
      config.extract[FileStreamDataSinkConfiguration] orElse
        config.extract[KafkaStreamDataSinkConfiguration] orElse
        config.extract[GenericStreamDataSinkConfiguration]
  }

  implicit val TriggerExtractor = new Extractor[Trigger] {
    val AcceptableValues = Seq("Continuous", "Once", "ProcessingTime")
    override def extract(config: Config, path: String): Trigger = {
      val triggerType = config.extract[String](s"$path.trigger.type").get
      val interval = config.extract[Option[String]](s"$path.trigger.interval").get
      triggerType.trim.toLowerCase() match {
        case "once" => Trigger.Once()
        case "continuous" =>
          require(interval.isDefined, "The interval must be defined for Continuous triggers.")
          Trigger.Continuous(interval.get)
        case "processingtime" =>
          require(interval.isDefined, "The interval must be defined for ProcessingTime triggers.")
          Trigger.ProcessingTime(interval.get)
        case tt => throw new IllegalArgumentException(s"The trigger.type '$tt' is not supported. " +
          s"The supported values are ${AcceptableValues.mkString(",", "', '", ",")}.")
      }
    }
  }

  /**
   * A Kafka subscription is defined by it's type (e.g. subscribe or subscribe patterns) and the
   * subscription itself (e.g. the actual topic name that this subscription is defining).
   * @param subscriptionType the type of the subscription (e.g. assign, subscribe or subscribe patterns)
   * @param subscription the topic name to subscribe to
   */
  case class KafkaSubscription(subscriptionType: String, subscription: String)
  implicit val KafkaSubscriptionExtractor = new Extractor[KafkaSubscription] {
    val AcceptableValues = Seq("assign", "subscribe", "subscribePattern")
    override def extract(config: Config, path: String): KafkaSubscription = {
      val subscriptionType = config.extract[String](s"$path.subscription.type")
        .ensure(new IllegalArgumentException(s"The subscription.type is not supported. " +
          s"The supported values are ${AcceptableValues.mkString(",", "', '", ",")}.").toNel)(st =>
          AcceptableValues.map(_.toLowerCase()).contains(st.toLowerCase)).get
      val subscription = config.extract[String](s"$path.subscription.value").get
      KafkaSubscription(subscriptionType, subscription)
    }
  }
}
