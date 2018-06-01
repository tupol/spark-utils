package org.apache.spark

/**
 * Expose the internal Apache Spark Logging until we find a better logging solution.
 *
 * TODO: Find a better logging solution.
 */
trait Logging extends org.apache.spark.internal.Logging
