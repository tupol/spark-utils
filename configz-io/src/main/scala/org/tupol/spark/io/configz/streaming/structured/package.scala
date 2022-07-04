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
package org.tupol.spark.io.configz.streaming

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger
import org.tupol.configz._
import org.tupol.spark.io.configz._
import org.tupol.spark.io.streaming.structured._
import scalaz.ValidationNel

import scala.util.{Failure, Success, Try}

package object structured {

  object FormatAwareStreamingSourceConfigurator extends Configurator[FormatAwareStreamingSourceConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, FormatAwareStreamingSourceConfiguration] =
      config.extract[FileStreamDataSourceConfiguration] orElse
        config.extract[KafkaStreamDataSourceConfiguration] orElse
        config.extract[GenericStreamDataSourceConfiguration]
  }

  object FormatAwareStreamingSinkConfigurator extends Configurator[FormatAwareStreamingSinkConfiguration] {
    override def validationNel(config: Config): ValidationNel[Throwable, FormatAwareStreamingSinkConfiguration] =
      config.extract[FileStreamDataSinkConfiguration] orElse
        config.extract[KafkaStreamDataSinkConfiguration] orElse
        config.extract[GenericStreamDataSinkConfiguration]
  }

  implicit val TriggerExtractor = new Extractor[Trigger] {
    val AcceptableValues = Seq("Continuous", "Once", "ProcessingTime")

    override def extract(config: Config, path: String): Try[Trigger] =
      for {
        triggerType <- config.extract[String](s"$path.trigger.type")
        interval <- config.extract[Option[String]](s"$path.trigger.interval")
        trigger <- triggerType.trim.toLowerCase() match {
          case "once" => Success(Trigger.Once())
          case "continuous" =>
            Try {
              require(interval.isDefined, "The interval must be defined for Continuous triggers.")
              Trigger.Continuous(interval.get)
            }
          case "processingtime" =>
            Try {
              require(interval.isDefined, "The interval must be defined for ProcessingTime triggers.")
              Trigger.ProcessingTime(interval.get)
            }
          case tt => Failure(new IllegalArgumentException(s"The trigger.type '$tt' is not supported. " +
            s"The supported values are ${AcceptableValues.mkString(",", "', '", ",")}."))
        }
      } yield trigger
  }

  implicit val KafkaSubscriptionExtractor = new Extractor[KafkaSubscription] {
    import org.tupol.spark.io.streaming.structured.KafkaSubscription.AcceptableTypes
    override def extract(config: Config, path: String): Try[KafkaSubscription] =
      for {
        subscriptionType <- config.extract[String](s"$path.subscription.type").ensure(new IllegalArgumentException(s"The subscription.type is not supported. " +
          s"The supported values are ${AcceptableTypes.mkString(",", "', '", ",")}.").toNel)(st =>
          AcceptableTypes.map(_.toLowerCase()).contains(st.toLowerCase))
        subscription <- config.extract[String](s"$path.subscription.value")
      } yield KafkaSubscription(subscriptionType, subscription)
  }
}
