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

package com.zto.fire.common.util

import org.slf4j.{Logger, LoggerFactory}

/**
 * 日志记录器
 *
 * @author ChengLong 2021-11-2 15:41:30
 * @since 2.2.0
 */
trait Logging {
  private lazy val log_ = LoggerFactory.getLogger(this.getClass)

  /**
   * 获取日志对象
   */
  protected def logger: Logger = log_

  protected def logInfo(msg: => String): Unit = {
    if (this.logger.isInfoEnabled) this.logger.info(msg)
  }

  protected def logDebug(msg: => String): Unit = {
    if (this.logger.isDebugEnabled) this.logger.debug(msg)
  }

  protected def logTrace(msg: => String): Unit = {
    if (this.logger.isTraceEnabled) this.logger.trace(msg)
  }

  protected def logWarning(msg: => String): Unit = {
    if (this.logger.isWarnEnabled) this.logger.warn(msg)
  }

  protected def logError(msg: => String): Unit = {
    if (this.logger.isErrorEnabled) this.logger.error(msg)
  }

  protected def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (this.logger.isInfoEnabled) this.logger.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable): Unit = {
    if (this.logger.isDebugEnabled) this.logger.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable): Unit = {
    if (this.logger.isTraceEnabled) this.logger.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable): Unit = {
    if (this.logger.isWarnEnabled) this.logger.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable): Unit = {
    if (this.logger.isErrorEnabled) this.logger.error(msg, throwable)
  }
}
