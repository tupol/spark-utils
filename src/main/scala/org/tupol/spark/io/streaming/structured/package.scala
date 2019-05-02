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
import org.tupol.spark.io.sources.SourceConfiguration
import org.tupol.utils.config._

package object structured {

  trait StreamingConfiguration
  trait StreamingSourceConfiguration extends SourceConfiguration with StreamingConfiguration

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
