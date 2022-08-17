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

import com.google.common.collect.EvictingQueue
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.analysis.ExceptionMsg
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.predef._
import org.slf4j.Logger

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}


/**
 * Fire框架异常总线，用于收集各引擎执行task过程中发生的异常信息
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-16 09:33
 */
object ExceptionBus extends Logging {
  // 用于保存收集而来的异常对象
  @transient
  private[this] lazy val queue = EvictingQueue.create[(String, Throwable, String)](FireFrameworkConf.exceptionBusSize)
  // 队列大小，对比queue.size有性能优势
  private[fire] lazy val queueSize = new AtomicInteger(0)
  // 异常总数计数器
  private[fire] lazy val exceptionCount = new AtomicLong(0)
  private[this] lazy val isStarted = new AtomicBoolean(false)
  this.sendToMQ

  /**
   * 周期性将异常堆栈信息发送到指定的MQ中，用于平台异常诊断
   */
  @Internal
  def sendToMQ: Unit = {
    if (!FireFrameworkConf.exceptionTraceEnable) return

    // 启动异步线程，定时将异常信息发送到指定的消息队列
    if (this.isStarted.compareAndSet(false, true)) {
      ThreadUtils.scheduleAtFixedRate({
        this.postException
      }, 0, 3, TimeUnit.SECONDS)

      // 注册回调，在jvm退出前将所有异常发送到mq中
      ShutdownHookManager.addShutdownHook() (() => {
        this.postException
        MQProducer.release
      })
    }
  }

  /**
   * 将异常信息投递到MQ中
   */
  private[this] def postException: Unit = {
    val mqUrl = FireFrameworkConf.exceptionTraceMQ
    val mqTopic = FireFrameworkConf.exceptionTraceMQTopic
    if (isEmpty(mqUrl, mqTopic)) return

    val msg = this.getAndClear
    if (msg._1.nonEmpty) {
      msg._1.foreach(t => {
        MQProducer.send(mqUrl, mqTopic, new ExceptionMsg(t._2, t._3).toString)
      })
      logger.debug(s"异常诊断：本轮发送异常共计${msg._1.size}个.")
    }
  }

  /**
   * 向异常总线中投递异常对象
   */
  def post(t: Throwable, sql: String = ""): Boolean = this.synchronized {
    exceptionCount.incrementAndGet()
    this.queue.offer((DateFormatUtils.formatCurrentDateTime(), t, sql))
  }

  /**
   * 获取并清空queue
   *
   * @return 异常集合
   */
  @Internal
  private[fire] def getAndClear: (List[(String, Throwable, String)], Long) = this.synchronized {
    val list = this.queue.toList
    this.queue.clear()
    queueSize.set(0)
    this.logger.debug(s"成功收集异常总线中的异常对象共计：${list.size}条，异常总线将会被清空.")
    (list, this.exceptionCount.get())
  }

  /**
   * 工具方法，用于打印异常信息
   */
  @Internal
  private[fire] def offAndLogError(logger: Logger, msg: String, t: Throwable, sql: String = ""): Unit = {
    this.post(t, sql)
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
