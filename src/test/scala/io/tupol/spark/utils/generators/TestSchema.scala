package io.tupol.spark.utils.generators

// Schema has a type and a sequence of records
case class TestMetadata(continuous: Boolean, label: Boolean)
case class TestRecord(name: String, `type`: String, nullable: Boolean, metadata: TestMetadata)
case class TestSchema(`type`: String, fields: Seq[TestRecord])
