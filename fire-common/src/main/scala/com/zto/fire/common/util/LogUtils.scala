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

import com.zto.fire.common.conf.FirePS1Conf
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.event.Level

/**
 * 日志工具类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-01 10:23
 */
object LogUtils extends Logging {

  /**
   * 以固定的开始与结束风格打日志
   *
   * @param logger
   * 日志记录器
   * @param title
   * 日志开始标题
   * @param style
   * 日志开始标题类型
   * @param level
   * 日志的级别
   * @param fun
   * 用户自定义的操作
   */
  def logStyle(logger: Logger, title: String = "", style: String = "-", level: Level = Level.INFO)(fun: Logger => Unit): Unit = {
    if (logger != null) {
      val styleRepeat = StringUtils.repeat(style, 19)
      val titleStart = styleRepeat + s"${FirePS1Conf.GREEN}> start: " + title + s" <${FirePS1Conf.DEFAULT}" + styleRepeat
      this.logLevel(logger, titleStart, level)
      fun(logger)
      val titleEnd = styleRepeat + s"${FirePS1Conf.GREEN}> end:   " + title + s" <${FirePS1Conf.DEFAULT}" + styleRepeat
      this.logLevel(logger, titleEnd, level)
    }
  }

  /**
   * 以固定的风格打印map中的内容
   */
  def logMap(logger: Logger = this.logger, map: Map[_, _], title: String): Unit = {
    if (logger != null && map != null && map.nonEmpty) {
      LogUtils.logStyle(logger, title)(logger => {
        map.foreach(kv => logger.info(s"---> ${kv._1} = ${kv._2}"))
      })
    }
  }

  /**
   * 根据指定的基本进行日志记录
   *
   * @param logger
   * 日志记录器
   * @param log
   * 日志内容
   * @param level
   * 日志的级别
   */
  def logLevel(logger: Logger, log: String, level: Level = Level.INFO, ps: String = null): Unit = {
    val logMsg = if (StringUtils.isNotBlank(ps)) s"$ps $log ${FirePS1Conf.DEFAULT}" else log
    level match {
      case Level.DEBUG => logger.debug(logMsg)
      case Level.INFO => logger.info(logMsg)
      case Level.WARN => logger.warn(logMsg)
      case Level.ERROR => logger.error(logMsg)
      case Level.TRACE => logger.trace(logMsg)
    }
  }
}
