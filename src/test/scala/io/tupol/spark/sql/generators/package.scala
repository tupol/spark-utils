package io.tupol.spark.sql

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

package object generators {

  // TODO values in metadata should be optional, find a way to set continuous/label as optional.
  val genMetadata: Gen[TestMetadata] = for {
    continuous <- arbitrary[Boolean]
    label <- arbitrary[Boolean]
  } yield TestMetadata(continuous, label)

  // TODO types can be also capitals, int, etc.
  val genRecord: Gen[TestRecord] = for {
    name <- Gen.alphaStr.filter(_.nonEmpty)
    t <- Gen.oneOf("string", "double", "integer", "float", "double", "long")
    nullable <- arbitrary[Boolean]
    metadata <- genMetadata
  } yield TestRecord(name, t, nullable, metadata)

  val genSchema: Gen[TestSchema] = for {
    fields <- Gen.nonEmptyListOf(genRecord)
  } yield TestSchema("struct", fields)

  val genInvalidSchema: Gen[TestSchema] = for {
    t <- Gen.alphaStr
    fields <- Gen.nonEmptyListOf(genRecord)
  } yield TestSchema(t, fields)

  val genInvalidRecord: Gen[TestRecord] = for {
    name <- Gen.alphaStr.filter(_.nonEmpty)
    t <- Gen.alphaStr.filter(s => !s.toLowerCase.equals("string") &&
      !s.toLowerCase.equals("double") &&
      !s.toLowerCase.equals("integer") &&
      !s.toLowerCase.equals("float") &&
      !s.toLowerCase.equals("double") &&
      !s.toLowerCase.equals("long"))
    nullable <- arbitrary[Boolean]
    metadata <- genMetadata
  } yield TestRecord(name, t, nullable, metadata)

  val genSchemaWithInvalidType: Gen[TestSchema] = for {
    fields <- Gen.nonEmptyListOf(genInvalidRecord)
  } yield TestSchema("struct", fields)

}
