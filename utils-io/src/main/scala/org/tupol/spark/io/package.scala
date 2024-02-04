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
package org.tupol.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.tupol.spark.io.sources.{GenericSourceConfiguration, JdbcSourceConfiguration}
import org.tupol.spark.io.streaming.structured.{FileStreamDataAwareSink, FileStreamDataSinkConfiguration, GenericStreamDataAwareSink, GenericStreamDataSinkConfiguration, KafkaStreamDataAwareSink, KafkaStreamDataSinkConfiguration }

/** Common IO utilities */
package object io {

  /** For things that should be aware of their format type */
  trait FormatAware {
    def format: FormatType
  }

  implicit val DataSourceFactory =
    new DataSourceFactory {
      override def apply[C <: DataSourceConfiguration](configuration: C): DataSource[C, DataFrameReader] =
        configuration match {
          //TODO There must be a better way to use the type system without the type cast
          case c: FileSourceConfiguration => FileDataSource(c).asInstanceOf[DataSource[C, DataFrameReader]]
          case c: JdbcSourceConfiguration => JdbcDataSource(c).asInstanceOf[DataSource[C, DataFrameReader]]
          case c: GenericSourceConfiguration => GenericDataSource(c).asInstanceOf[DataSource[C, DataFrameReader]]
          case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
        }
    }


  implicit val DataAwareSinkFactory =
    new DataAwareSinkFactory {
      override def apply[C <: DataSinkConfiguration, WR, WO](configuration: C, data: DataFrame): DataAwareSink[C, WR, WO] =
        data.isStreaming match {
          case false =>
            configuration match {
              //TODO There must be a better way to use the type system without the type cast
              case c: FileSinkConfiguration => FileDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WR, WO]]
              case c: JdbcSinkConfiguration => JdbcDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WR, WO]]
              case c: GenericSinkConfiguration => GenericDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WR, WO]]
              case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
            }
          case true =>
            configuration match {
              //TODO There must be a better way to use the type system without the type cast
              case c: FileStreamDataSinkConfiguration => FileStreamDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WR, WO]]
              case c: KafkaStreamDataSinkConfiguration => KafkaStreamDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WR, WO]]
              case c: GenericStreamDataSinkConfiguration => GenericStreamDataAwareSink(c, data).asInstanceOf[DataAwareSink[C, WR, WO]]
              case u => throw new IllegalArgumentException(s"Unsupported configuration type ${u.getClass}.")
            }
        }
    }

  private [io] def appendOptionalMaps[K, V](map1: Option[Map[K, V]], map2: Option[Map[K, V]]): Option[Map[K, V]] =
    (map1, map2) match {
      case (Some(o1), Some(o2)) => Some(o1 ++ o2)
      case ((Some(o1)), None) => Some(o1)
      case (None, Some(o2)) => Some(o2)
      case _ => None
    }

}
