package org.tupol.spark.io.pureconf.streaming.structured

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.tupol.spark.io.{FormatType, PartitionsConfiguration}
import org.tupol.spark.io.streaming.structured.{FileStreamDataSinkConfiguration, FileStreamDataSourceConfiguration, FormatAwareStreamingSinkConfiguration, FormatAwareStreamingSourceConfiguration, GenericStreamDataSinkConfiguration, GenericStreamDataSourceConfiguration, KafkaStreamDataSinkConfiguration, KafkaStreamDataSourceConfiguration, KafkaSubscription}
import org.tupol.spark.io.streaming.structured.KafkaSubscription.AcceptableTypes
import pureconfig.error.FailureReason
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader}
import pureconfig.generic.ProductHint

object readers {

  implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val TriggerReader: ConfigReader[Trigger] = {
    import pureconfig.generic.auto._
    ConfigReader[StreamingTrigger].map(StreamingTrigger.toTrigger)
  }

  implicit val KafkaSubscriptionReader: ConfigReader[KafkaSubscription] =
    ConfigReader.fromCursor[KafkaSubscription] { cur =>
      for {
        objCur <- cur.asObjectCursor
        subscriptionType <- objCur.atKey("type").flatMap(_.asString)
        subscription <- objCur.atKey("value").flatMap(_.asString)

      } yield KafkaSubscription(subscriptionType, subscription)
    }.ensure(
      subscription => KafkaSubscription.AcceptableTypes.contains(subscription.subscriptionType),
      subscription => s"The subscription.type '${subscription.subscriptionType}' is not supported. " +
        s"The supported values are ${AcceptableTypes.mkString("'", "', '", "'")}."
    )

  implicit val FileStreamDataSourceConfigurationReader: ConfigReader[FileStreamDataSourceConfiguration] = {

    import org.tupol.spark.io.pureconf.readers._
    ConfigReader.fromCursor[FileStreamDataSourceConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        formatCur <- objCur.atKey("format")
        format <- FormatTypeReader.from(formatCur)
        _ <- if(FormatType.AcceptableFileFormats.contains(format)) Right(())
        else formatCur.failed(new FailureReason {
          override def description: String =
            s"The provided format '$format' is unsupported for a file data source. " +
              s"Supported formats are: ${FormatType.AcceptableFileFormats.mkString("'", "', '", "'")}"
        })
        pathCur <- objCur.atKey("path")
        path <- pathCur.asString
        sourceConfig <- SourceConfigurationReader.from(objCur)
      } yield FileStreamDataSourceConfiguration(path, sourceConfig)
    }
  }

  implicit val GenericStreamDataSourceConfigurationReader: ConfigReader[GenericStreamDataSourceConfiguration] = {
    import org.tupol.spark.io.pureconf.readers._
    ConfigReader
      .forProduct3[GenericStreamDataSourceConfiguration, FormatType, Option[Map[String, String]], Option[StructType]]("format", "options", "schema")(GenericStreamDataSourceConfiguration(_, _, _))
      .ensure(conf => FormatType.AcceptableStreamingFormats.contains(conf.format),
        conf =>
          s"The provided format '${conf.format}' is unsupported for a file data source. " +
            s"Supported formats are: ${FormatType.AcceptableStreamingFormats.mkString("'", "', '", "'")}"

      )
  }

  implicit val GenericStreamDataSinkConfigurationReader: ConfigReader[GenericStreamDataSinkConfiguration] = {
    import org.tupol.spark.io.pureconf.readers._
    ConfigReader
      .forProduct6[GenericStreamDataSinkConfiguration, FormatType, Option[Map[String, String]], Option[String], Option[Trigger],
        Option[PartitionsConfiguration], Option[String]]("format", "options", "queryName", "trigger",
        "partition", "outputMode")(GenericStreamDataSinkConfiguration(_, _, _, _, _, _))
      .ensure(conf => FormatType.AcceptableStreamingFormats.contains(conf.format),
        conf =>
          s"The provided format '${conf.format}' is unsupported for a file data sink. " +
            s"Supported formats are: ${FormatType.AcceptableStreamingFormats.mkString("'", "', '", "'")}"
      )
  }

  implicit val FileStreamDataSinkConfigurationReader: ConfigReader[FileStreamDataSinkConfiguration] =
    ConfigReader.fromCursor[FileStreamDataSinkConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        pathCur <- objCur.atKey("path")
        path <- pathCur.asString
        cpl <- ConfigReader[Option[String]].from(objCur.atKeyOrUndefined("checkpointLocation"))
        sinkConfig <- GenericStreamDataSinkConfigurationReader.from(objCur)
      } yield FileStreamDataSinkConfiguration(path, sinkConfig, cpl)
    }

  implicit val KafkaStreamDataSinkConfigurationReader: ConfigReader[KafkaStreamDataSinkConfiguration] = {
    ConfigReader.fromCursor[KafkaStreamDataSinkConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        kafkaBootstrapServers <- ConfigReader[String].from(objCur.atKeyOrUndefined("kafkaBootstrapServers"))
        topic <- ConfigReader[Option[String]].from(objCur.atKeyOrUndefined("topic"))
        checkpointLocation <- ConfigReader[Option[String]].from(objCur.atKeyOrUndefined("checkpointLocation"))
        genericConfig <- GenericStreamDataSinkConfigurationReader.from(objCur)
      } yield KafkaStreamDataSinkConfiguration(kafkaBootstrapServers, genericConfig, topic, checkpointLocation)
    }
  }

  implicit val KafkaStreamDataSourceConfigurationReader: ConfigReader[KafkaStreamDataSourceConfiguration] = {
    import org.tupol.spark.io.pureconf.readers._
    ConfigReader.fromCursor[KafkaStreamDataSourceConfiguration] { cur =>
      for {
        objCur <- cur.asObjectCursor
        kafkaBootstrapServers <- ConfigReader[String].from(objCur.atKeyOrUndefined("kafkaBootstrapServers"))
        startingOffsets <- ConfigReader[Option[String]].from(objCur.atKeyOrUndefined("startingOffsets"))
        endingOffsets <- ConfigReader[Option[String]].from(objCur.atKeyOrUndefined("endingOffsets"))
        failOnDataLoss <- ConfigReader[Option[Boolean]].from(objCur.atKeyOrUndefined("failOnDataLoss"))
        kafkaConsumerPollTimeoutMs <- ConfigReader[Option[Long]].from(objCur.atKeyOrUndefined("kafkaConsumerPollTimeoutMs"))
        fetchOffsetNumRetries <- ConfigReader[Option[Int]].from(objCur.atKeyOrUndefined("fetchOffsetNumRetries"))
        fetchOffsetRetryIntervalMs <- ConfigReader[Option[Long]].from(objCur.atKeyOrUndefined("fetchOffsetRetryIntervalMs"))
        maxOffsetsPerTrigger <- ConfigReader[Option[Long]].from(objCur.atKeyOrUndefined("maxOffsetsPerTrigger"))
        schema <- ConfigReader[Option[StructType]].from(objCur.atKeyOrUndefined("schema"))
        subscription <- KafkaSubscriptionReader.from(objCur.atKeyOrUndefined("subscription"))
        options <- ConfigReader[Option[Map[String, String]]].from(objCur.atKeyOrUndefined("options"))
      } yield KafkaStreamDataSourceConfiguration(
        kafkaBootstrapServers, subscription, startingOffsets, endingOffsets, failOnDataLoss,
        kafkaConsumerPollTimeoutMs, fetchOffsetNumRetries, fetchOffsetRetryIntervalMs,
        maxOffsetsPerTrigger, schema, options.getOrElse(Map())
      )
    }
  }

  implicit val FormatAwareStreamingSourceConfigurationReader: ConfigReader[FormatAwareStreamingSourceConfiguration] =
    ConfigReader[FileStreamDataSourceConfiguration]
      .orElse(ConfigReader[KafkaStreamDataSourceConfiguration])
      .orElse(ConfigReader[GenericStreamDataSourceConfiguration])

  implicit val FormatAwareStreamingSinkConfigurationReader: ConfigReader[FormatAwareStreamingSinkConfiguration] =
    ConfigReader[FileStreamDataSinkConfiguration]
      .orElse(ConfigReader[KafkaStreamDataSinkConfiguration])
      .orElse(ConfigReader[GenericStreamDataSinkConfiguration])

}
