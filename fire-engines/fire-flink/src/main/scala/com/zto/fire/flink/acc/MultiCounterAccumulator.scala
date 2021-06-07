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

package com.zto.fire.flink.acc

import java.util.concurrent.ConcurrentHashMap

import com.zto.fire.predef._
import org.apache.flink.api.common.accumulators.{Accumulator, SimpleAccumulator}


/**
 * flink 自定义多值累加器
 *
 * @author ChengLong 2020年1月11日 13:58:15
 * @since 0.4.1
 */
private[fire] class MultiCounterAccumulator extends SimpleAccumulator[ConcurrentHashMap[String, Long]] {
  private[fire] val multiCounter = new ConcurrentHashMap[String, Long]()

  /**
   * 向累加器中添加新的值
   *
   * @param value
   */
  override def add(value: ConcurrentHashMap[String, Long]): Unit = {
    this.mergeMap(value)
  }

  /**
   * 添加一个值到累加器中
   */
  def add(kv: (String, Long)): Unit = {
    if (kv != null) {
      this.multiCounter.put(kv._1, this.multiCounter.getOrDefault(kv._1, 0) + kv._2)
    }
  }

  /**
   * 获取当前本地的累加器中的值
   *
   * @return
   * 当前jvm中的累加器值，非全局
   */
  override def getLocalValue: ConcurrentHashMap[String, Long] = {
    this.multiCounter
  }

  /**
   * 清空当前本地的累加值
   */
  override def resetLocal(): Unit = {
    this.multiCounter.clear()
  }

  /**
   * 合并两个累加器中的值
   */
  override def merge(other: Accumulator[ConcurrentHashMap[String, Long], ConcurrentHashMap[String, Long]]): Unit = {
    this.mergeMap(other.getLocalValue)
  }

  /**
   * 用于合并数据到累加器的map中
   * 存在的累加，不存在的直接添加
   */
  private[this] def mergeMap(value: ConcurrentHashMap[String, Long]): Unit = {
    if (noEmpty(value)) {
      value.foreach(kv => {
        this.multiCounter.put(kv._1, this.multiCounter.getOrDefault(kv._1, 0) + kv._2)
      })
    }
  }
}
