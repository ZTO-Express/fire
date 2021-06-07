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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.google.common.collect.EvictingQueue
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.predef._
import org.slf4j.{Logger, LoggerFactory}


/**
 * Fire框架异常总线，用于收集各引擎执行task过程中发生的异常信息
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-16 09:33
 */
object ExceptionBus {
  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)
  // 用于保存收集而来的异常对象
  @transient
  private[this] lazy val queue = EvictingQueue.create[(String, Throwable)](FireFrameworkConf.exceptionBusSize)
  // 队列大小，对比queue.size有性能优势
  private[fire] lazy val queueSize = new AtomicInteger(0)
  // 异常总数计数器
  private[fire] lazy val exceptionCount = new AtomicLong(0)

  /**
   * 向异常总线中投递异常对象
   */
  def post(t: Throwable): Boolean = this.synchronized {
    exceptionCount.incrementAndGet()
    this.queue.offer((DateFormatUtils.formatCurrentDateTime(), t))
  }

  /**
   * 获取并清空queue
   *
   * @return 异常集合
   */
  @Internal
  private[fire] def getAndClear: (List[(String, Throwable)], Long) = this.synchronized {
    val list = this.queue.toList
    this.queue.clear()
    queueSize.set(0)
    this.logger.warn(s"成功收集异常总线中的异常对象共计：${list.size}条，异常总线将会被清空.")
    (list, this.exceptionCount.get())
  }

  /**
   * 工具方法，用于打印异常信息
   */
  @Internal
  private[fire] def offAndLogError(logger: Logger, msg: String, t: Throwable): Unit = {
    this.post(t)
    if (noEmpty(msg)) {
      if (logger != null) logger.error(msg, t) else t.printStackTrace()
    }
  }

  /**
   * 获取Throwable的堆栈信息
   */
  def stackTrace(t: Throwable): String = {
    if (t == null) return ""
    val stackTraceInfo = new StringBuilder()
    stackTraceInfo.append(t.toString + "\n")
    t.getStackTrace.foreach(trace => stackTraceInfo.append("\tat " + trace + "\n"))
    stackTraceInfo.toString
  }

}
