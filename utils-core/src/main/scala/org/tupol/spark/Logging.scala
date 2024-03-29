/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tupol.spark

/**
 * Basic logging trait;
 * This code contains major parts of the original Logging trait from org.apache.spark.internal.Logging.
 * In the future this logging trait will be changed, but for now it will do.
 * The original license was preserved.
 */
import org.slf4j.{ Logger, LoggerFactory }

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
 */
trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  protected def logName =
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null)
      log_ = LoggerFactory.getLogger(logName)
    log_
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String): Unit =
    if (log.isInfoEnabled) log.info(msg)

  protected def logDebug(msg: => String): Unit =
    if (log.isDebugEnabled) log.debug(msg)

  protected def logTrace(msg: => String): Unit =
    if (log.isTraceEnabled) log.trace(msg)

  protected def logWarning(msg: => String): Unit =
    if (log.isWarnEnabled) log.warn(msg)

  protected def logError(msg: => String): Unit =
    if (log.isErrorEnabled) log.error(msg)

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable): Unit =
    if (log.isInfoEnabled) log.info(msg, throwable)

  protected def logInfo(throwable: Throwable): Unit =
    logInfo(throwable.getMessage, throwable)

  protected def logDebug(msg: => String, throwable: Throwable): Unit =
    if (log.isDebugEnabled) log.debug(msg, throwable)

  protected def logDebug(throwable: Throwable): Unit =
    logDebug(throwable.getMessage, throwable)

  protected def logTrace(msg: => String, throwable: Throwable): Unit =
    if (log.isTraceEnabled) log.trace(msg, throwable)

  protected def logTrace(throwable: Throwable): Unit =
    logTrace(throwable.getMessage, throwable)

  protected def logWarning(msg: => String, throwable: Throwable): Unit =
    if (log.isWarnEnabled) log.warn(msg, throwable)

  protected def logWarning(throwable: Throwable): Unit =
    logWarning(throwable.getMessage, throwable)

  protected def logError(msg: => String, throwable: Throwable): Unit =
    if (log.isErrorEnabled) log.error(msg, throwable)

  protected def logError(throwable: Throwable): Unit =
    logError(throwable.getMessage, throwable)

  protected def isTraceEnabled(): Boolean =
    log.isTraceEnabled

}
