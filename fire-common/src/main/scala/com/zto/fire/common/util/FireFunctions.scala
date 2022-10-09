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
import com.zto.fire.common.util.UnitFormatUtils.{TimeUnitEnum, readable}
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger

import scala.util.Try

/**
 * 常用的函数库
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-16 15:45
 */
trait FireFunctions extends Serializable with Logging  {
  private[this] lazy val tryLog = ""
  private[this] lazy val catchLog = "执行try的过程中发生异常"
  private[this] lazy val finallyCatchLog = "执行finally过程中发生异常"

  /**
   * 重试指定的函数fn retryNum次
   * 当fn执行失败时，会根据设置的重试次数自动重试retryNum次
   * 每次重试间隔等待duration(毫秒)
   *
   * @param retryNum
   * 指定重试的次数
   * @param duration
   * 重试的间隔时间（ms）
   * @param fun
   * 重试的函数或方法
   * @tparam T
   * fn执行后返回的数据类型
   * @return
   * 返回fn执行结果
   */
  def retry[T](retryNum: Long = 3, duration: Long = 3000)(fun: => T): T = {
    var count = 1L

    def redo[T](retryNum: Long, duration: Long)(fun: => T): T = {
      Try {
        fun
      } match {
        case util.Success(x) => x
        case _ if retryNum > 1 => {
          Thread.sleep(duration)
          count += 1
          logger.info(s"${FirePS1Conf.RED}第${count}次执行. 时间:${DateFormatUtils.formatCurrentDateTime()}. 间隔:${duration}.${FirePS1Conf.DEFAULT}")
          redo(retryNum - 1, duration)(fun)
        }
        case util.Failure(e) => throw e
      }
    }

    redo(retryNum, duration)(fun)
  }

  /**
   * 尝试执行block中的逻辑，如果出现异常，则记录日志
   *
   * @param block
   * try的具体逻辑
   * @param logger
   * 日志记录器
   * @param catchLog
   * 日志内容
   * @param hook
   * 是否将捕获到的异常信息发送到消息队列
   */
  def tryWithLog(block: => Unit)(logger: Logger = this.logger, tryLog: String = tryLog, catchLog: String = catchLog, isThrow: Boolean = false, hook: Boolean = true): Unit = {
    try {
      elapsed(tryLog, logger)(block)
    } catch {
      case t: Throwable => {
        if (hook) ExceptionBus.offAndLogError(logger, catchLog, t)
        if (isThrow) throw t
      }
    }
  }

  /**
   * 尝试执行block中的逻辑，如果出现异常，则记录日志，并将执行结果返回
   *
   * @param block
   * try的具体逻辑
   * @param logger
   * 日志记录器
   * @param catchLog
   * 日志内容
   * @param hook
   * 是否将捕获到的异常信息发送到消息队列
   */
  def tryWithReturn[T](block: => T)(logger: Logger = this.logger, tryLog: String = tryLog, catchLog: String = catchLog, hook: Boolean = true): T = {
    try {
      elapsed[T](tryLog, logger)(block)
    } catch {
      case t: Throwable => {
        if (hook) ExceptionBus.offAndLogError(logger, catchLog, t)
        throw t
      }
    }
  }

  /**
   * 执行用户指定的try/catch/finally逻辑
   *
   * @param block
   * try 代码块
   * @param finallyBlock
   * finally 代码块
   * @param logger
   * 日志记录器
   * @param catchLog
   * 当执行try过程中发生异常时打印的日志内容
   * @param finallyCatchLog
   * 当执行finally代码块过程中发生异常时打印的日志内容
   * @param hook
   * 是否将捕获到的异常信息发送到消息队列
   */
  def tryFinallyWithReturn[T](block: => T)(finallyBlock: => Unit)(logger: Logger = this.logger, tryLog: String = tryLog, catchLog: String = catchLog, finallyCatchLog: String = finallyCatchLog, hook: Boolean = true): T = {
    try {
      elapsed[T](tryLog, logger)(block)
    } catch {
      case t: Throwable =>
        if (hook) ExceptionBus.offAndLogError(logger, catchLog, t)
        throw t
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (hook) ExceptionBus.offAndLogError(logger, catchLog, t)
          throw t
      }
    }
  }

  /**
   * 执行用户指定的try/catch/finally逻辑
   *
   * @param block
   * try 代码块
   * @param finallyBlock
   * finally 代码块
   * @param logger
   * 日志记录器
   * @param catchLog
   * 当执行try过程中发生异常时打印的日志内容
   * @param finallyCatchLog
   * 当执行finally代码块过程中发生异常时打印的日志内容
   * @param hook
   * 是否将捕获到的异常信息发送到消息队列
   */
  def tryFinally(block: => Unit)(finallyBlock: => Unit)(logger: Logger = this.logger, tryLog: String = tryLog, catchLog: String = catchLog, finallyCatchLog: String = finallyCatchLog, hook: Boolean = true): Unit = {
    try {
      elapsed[Unit](tryLog, logger)(block)
    } catch {
      case t: Throwable => if (hook) ExceptionBus.offAndLogError(logger, catchLog, t)
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable => if (hook) ExceptionBus.offAndLogError(logger, catchLog, t)
      }
    }
  }

  /**
   * 获取当前系统时间（ms）
   */
  def currentTime: Long = System.currentTimeMillis

  /**
   * 以人类可读的方式计算耗时（ms）
   *
   * @param beginTime
   * 开始时间
   * @return
   * 耗时
   */
  def elapsed(beginTime: Long): String = readable(currentTime - beginTime, TimeUnitEnum.MS)

  /**
   * 用于统计指定代码块执行的耗时时间
   *
   * @param msg
   * 用于描述当前代码块的用户
   * @param logger
   * 日志记录器
   * @param threshold
   * 执行代码块耗时超过给定的阈值时才记录日志
   * @param block
   * try的具体逻辑
   */
  def elapsed[T](msg: String, logger: Logger = this.logger, threshold: Long = 0)(block: => T): T = {
    val startTime = this.currentTime
    val retVal = block
    if (StringUtils.isNotBlank(msg) && (System.currentTimeMillis() - startTime) >= threshold) logger.info(s"${msg}, Elapsed：${elapsed(startTime)}")
    retVal
  }
}
