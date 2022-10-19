package org.tupol.spark.io.configz.streaming.structured

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.configz._
import org.tupol.spark.io.FormatType._
import org.tupol.configz._
import org.tupol.spark.io.streaming.structured.{GenericStreamDataSinkConfiguration, KafkaStreamDataSinkConfiguration}

class KafkaStreamDataSinkConfigurationSpec extends AnyFunSuite with Matchers {

  test("Successfully extract a minimal KafkaStreamDataSinkConfiguration out of a configuration string") {

    val configStr =
      """
        |format=kafka
        |kafkaBootstrapServers=test_server
        |options {
        |   key1: val1
        |   key2: val2
        |}
        |trigger: {
        |   type="continuous"
        |   interval="12 seconds"
        |}
        |queryName=testQueryName
        |partition.columns=["col1", "col2"]
        |topic=my_topic
        |outputMode=testOutputMode
        |checkpointLocation=myCheckpointLocation
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(
      Kafka,
      Map("key1" -> "val1", "key2" -> "val2", "topic" -> "my_topic",
        "kafka.bootstrap.servers" -> "test_server", "checkpointLocation" -> "myCheckpointLocation"),
      Some("testQueryName"), Some(Trigger.Continuous(12000)), Seq("col1", "col2"), Some("testOutputMode"))
    config.extract[KafkaStreamDataSinkConfiguration].get.generic shouldBe expected
  }

  test("Successfully extract a minimal KafkaStreamDataSinkConfiguration out of a configuration string with empty options") {

    val configStr =
      """
        |format=kafka
        |kafkaBootstrapServers=test_server
        |options {
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Kafka, Map("kafka.bootstrap.servers" -> "test_server"))
    config.extract[KafkaStreamDataSinkConfiguration].get.generic shouldBe expected
  }

  test("Successfully extract a minimal KafkaStreamDataSinkConfiguration out of a configuration string with options") {

    val configStr =
      """
        |format=kafka
        |kafkaBootstrapServers=test_server
        |options {
        | key1: val1
        | key2: val2
        |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expected = GenericStreamDataSinkConfiguration(Kafka, Map("key1" -> "val1", "key2" -> "val2",
      "kafka.bootstrap.servers" -> "test_server"))
    config.extract[KafkaStreamDataSinkConfiguration].get.generic shouldBe expected
  }

  test("Failed to extract KafkaStreamDataSinkConfiguration out of an empty configuration string") {

    val configStr =
      """
        |format=kafka
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    a[ConfigurationException] shouldBe thrownBy(config.extract[KafkaStreamDataSinkConfiguration].get)
  }
}
