package org.tupol.spark.implicits
import org.json4s.Extraction
import org.tupol.spark.utils.TimeSerializers


object product {

  /** Product decorator */
  implicit class ProductOps(val product: Product) extends AnyRef {
    import org.json4s.jackson.Serialization
    import org.json4s.NoTypeHints

    /** Convert the product into a map keyed by field name */
    def toMap: Map[String, Any] = {
      val formats = Serialization.formats(NoTypeHints) ++ TimeSerializers
      Extraction.decompose(product)(formats).values.asInstanceOf[Map[String, Any]]
    }
  }

}
