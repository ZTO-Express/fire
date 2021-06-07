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

package com.zto.fire.spark.acc

import java.util.concurrent.ConcurrentHashMap

import com.zto.fire._
import com.zto.fire.common.conf.FireFrameworkConf
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.AccumulatorV2


/**
  * 多值累加器
  *
  * @author ChengLong 2019-8-16 16:56:06
  */
private[fire] class MultiCounterAccumulator extends AccumulatorV2[(String, Long), ConcurrentHashMap[String, Long]] {
  private[fire] val multiCounter = new ConcurrentHashMap[String, Long]()
  // 判断是否打开多值累加器
  private lazy val isEnable = FireFrameworkConf.accEnable && FireFrameworkConf.accMultiCounterEnable

  /**
    * 用于判断当前累加器是否为空
    *
    * @return
    * true: 空 false：不为空
    */
  override def isZero: Boolean = this.multiCounter.size() == 0

  /**
    * 用于复制一个新的累加器实例
    *
    * @return
    * 新的累加器实例对象
    */
  override def copy(): AccumulatorV2[(String, Long), ConcurrentHashMap[String, Long]] = {
    val tmpAcc = new MultiCounterAccumulator
    tmpAcc.multiCounter.putAll(this.multiCounter)
    tmpAcc
  }

  /**
    * 用于重置累加器
    */
  override def reset(): Unit = this.multiCounter.clear

  /**
    * 用于添加新的数据到累加器中
    *
    * @param kv
    * 累加值的key和value
    */
  override def add(kv: (String, Long)): Unit = this.mergeMap(kv)

  /**
    * 用于合并数据到累加器的map中
    * 存在的累加，不存在的直接添加
    *
    * @param kv
    * 累加值的key和value
    */
  private[this] def mergeMap(kv: (String, Long)): Unit = {
    if (this.isEnable && kv != null && StringUtils.isNotBlank(kv._1)) {
      this.multiCounter.put(kv._1, this.multiCounter.getOrDefault(kv._1, 0) + kv._2)
    }
  }

  /**
    * 用于合并executor端的map到driver端
    *
    * @param other
    * executor端的map
    */
  override def merge(other: AccumulatorV2[(String, Long), ConcurrentHashMap[String, Long]]): Unit = {
    val otherMap = other.value
    if (otherMap != null && otherMap.nonEmpty) {
      otherMap.foreach(kv => {
        this.mergeMap(kv)
      })
    }
  }

  /**
    * 用于driver端获取累加器（map）中的值
    *
    * @return
    * 累加器中的值
    */
  override def value: ConcurrentHashMap[String, Long] = this.multiCounter
}
