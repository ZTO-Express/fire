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

package com.zto.fire.flink.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent._
import scala.util.control._

/**
 * fire框架基础的flink sink类
 * 提供按批次、固定频率定时flush、checkpoint等功能
 * JDBC sink、HBase sink可继承自此类，并实现自己的flush方法，完成数据的sink
 *
 * @param batch         每批大小，达到该阈值将批量sink到目标组件
 * @param flushInterval 每隔多久刷新一次到目标组件（ms）
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-05-21 15:27
 */
abstract class BaseSink[IN, OUT](batch: Int, flushInterval: Long) extends RichSinkFunction[IN] with CheckpointedFunction {
  protected var maxRetry: Long = 3
  private var flushException: Exception = _
  @transient protected var scheduler: ScheduledExecutorService = _
  @transient protected var scheduledFuture: ScheduledFuture[_] = _
  protected lazy val closed = new AtomicBoolean(false)
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)
  @transient protected lazy val buffer = new CopyOnWriteArrayList[OUT]()

  /**
   * 初始化定时调度器，用于定时flush数据到目标组件
   */
  override def open(parameters: Configuration): Unit = {
    if (this.flushInterval > 0 && batch > 0) {
      this.scheduler = Executors.newScheduledThreadPool(1)
      if (this.scheduler != null) {
        this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Runnable {
          override def run(): Unit = this.synchronized {
            if (closed.get()) return
            flush
          }
        }, this.flushInterval, this.flushInterval, TimeUnit.MILLISECONDS)
      }
    }
  }

  /**
   * 将数据sink到目标组件
   * 不同的组件需定义该flush逻辑实现不同组件的flush操作
   */
  def sink: Unit = {
    // sink逻辑
  }

  /**
   * 将数据构建成sink的格式
   */
  def map(value: IN): OUT

  /**
   * 关闭资源
   * 1. 关闭定时flush线程池
   * 2. 将缓冲区中的数据flush到目标组件
   */
  override def close(): Unit = {
    if (closed.get()) return
    closed.compareAndSet(false, true)

    this.checkFlushException

    if (this.scheduledFuture != null) {
      scheduledFuture.cancel(false)
      this.scheduler.shutdown()
    }

    if (this.buffer.size > 0) {
      this.flush
    }
  }

  /**
   * 将数据sink到缓冲区中
   */
  override def invoke(value: IN, context: SinkFunction.Context): Unit = {
    this.checkFlushException

    val out = this.map(value)
    if (out != null) this.buffer.add(out)
    if (this.buffer.size >= this.batch) {
      this.flush
    }
  }

  /**
   * 内部的flush，调用用户定义的flush方法
   * 并清空缓冲区，将缓冲区大小归零
   */
  def flush: Unit = this.synchronized {
    this.checkFlushException

    if (this.buffer != null && this.buffer.size > 0) {
      this.logger.info(s"执行flushInternal操作 sink.size=${this.buffer.size()} batch=${this.batch} flushInterval=${this.flushInterval}")
      val loop = new Breaks
      loop.breakable {
        if (this.maxRetry < 1) this.maxRetry = 1
        for (i <- 1L to this.maxRetry) {
          try {
            this.sink
            this.buffer.clear()
            loop.break
          } catch {
            case e: Exception => {
              this.logger.error(s"执行flushInternal操作失败，正在进行第${i}次重试。", e)
              if (i >= this.maxRetry) {
                this.flushException = e
              }
              Thread.sleep(1000 * i)
            }
          }
        }
      }
    }
  }

  /**
   * checkpoint时将数据全部flush
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    this.flush
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    // initializeState
  }

  /**
   * 用于检测在flush过程中是否有异常，如果存在异常，则不再flush
   */
  private def checkFlushException: Unit = {
    if (flushException != null) throw new RuntimeException(s"${this.getClass.getSimpleName} writing records failed.", flushException)
  }
}
