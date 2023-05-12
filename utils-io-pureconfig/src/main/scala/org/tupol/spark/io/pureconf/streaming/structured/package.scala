package org.tupol.spark.io.pureconf.streaming

import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.Duration

package object structured {

  sealed trait StreamingTrigger

  object StreamingTrigger {
    // TODO: Add AvailableNow when bumping the Spark version
    // case object AvailableNow extends StreamingTrigger
    case object Once extends StreamingTrigger
    case class Continuous(interval: Duration) extends StreamingTrigger
    case class ProcessingTime(interval: Duration) extends StreamingTrigger

    implicit def toTrigger(trigger: StreamingTrigger): Trigger = trigger match {
      // case StreamingTrigger.AvailableNow => Trigger.AvailableNow()
      case StreamingTrigger.Once => Trigger.Once()
      case StreamingTrigger.Continuous(interval) => Trigger.Continuous(interval)
      case StreamingTrigger.ProcessingTime(interval) => Trigger.ProcessingTime(interval)
    }
  }


}
