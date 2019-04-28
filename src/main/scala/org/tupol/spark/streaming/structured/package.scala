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
package org.tupol.spark.streaming

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.tupol.spark.io.{ DataAwareSink, DataAwareSinkFactory, DataSinkConfiguration, DataSource, DataSourceConfiguration, DataSourceFactory }
import org.tupol.utils.config._

package object structured {

  trait StreamingConfiguration

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

  implicit val DataSourceFactory =
    new DataSourceFactory {
      override def apply[C <: DataSourceConfiguration](configuration: C): DataSource[C] =
        configuration match {
          //TODO There must be a better way to use the type system without the type cast
          case c: FileStreamDataSourceConfiguration => FileStreamDataSource(c).asInstanceOf[DataSource[C]]
          case c: GenericStreamDataSourceConfiguration => GenericStreamDataSource(c).asInstanceOf[DataSource[C]]
          case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
        }
    }

  implicit val DataAwareSinkFactory =
    new DataAwareSinkFactory {
      override def apply[C <: DataSinkConfiguration, WO](configuration: C, data: DataFrame): DataAwareSink[C, WO] =
        configuration match {
          //TODO There must be a better way to use the type system without the type cast
          case c: GenericStreamDataSinkConfiguration => GenericStreamDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WO]]
          case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
        }
    }

}
