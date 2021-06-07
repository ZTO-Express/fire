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

import com.google.common.collect.HashBasedTable
import com.zto.fire._
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.DateFormatUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.AccumulatorV2

import java.util.Date
import scala.collection.mutable

/**
 * timer累加器，对相同的key进行分钟级维度累加
 *
 * @author ChengLong 2019-8-21 14:22:12
 */
private[fire] class MultiTimerAccumulator extends AccumulatorV2[(String, Long, String), HashBasedTable[String, String, Long]] {
  private[fire] lazy val timerCountTable = HashBasedTable.create[String, String, Long]
  // 用于记录上次清理过期累加数据的时间
  private var lastClearTime = new Date
  // 判断是否打开多时间维度累加器
  private lazy val isEnable = FireFrameworkConf.accEnable && FireFrameworkConf.accMultiCounterEnable

  /**
   * 用于判断当前累加器是否为空
   *
   * @return
   * true: 空 false：不为空
   */
  override def isZero: Boolean = this.timerCountTable.size() == 0

  /**
   * 用于复制一个新的累加器实例
   *
   * @return
   * 新的累加器实例对象
   */
  override def copy(): AccumulatorV2[(String, Long, String), HashBasedTable[String, String, Long]] = {
    val tmpAcc = new MultiTimerAccumulator
    tmpAcc.timerCountTable.putAll(this.timerCountTable)
    tmpAcc
  }

  /**
   * 用于重置累加器
   */
  override def reset(): Unit = this.timerCountTable.clear

  /**
   * 用于添加新的数据到累加器中
   *
   * @param kv
   * 累加值的key、value和时间的schema，默认为yyyy-MM-dd HH:mm:00
   */
  override def add(kv: (String, Long, String)): Unit = {
    if (!isEnable || kv == null) return
    val schema = if (StringUtils.isBlank(kv._3)) {
      DateFormatUtils.TRUNCATE_MIN
    } else kv._3
    if (StringUtils.isNotBlank(kv._1)) {
      this.mergeTable(kv._1, DateFormatUtils.formatCurrentBySchema(schema), kv._2)
    }
  }

  /**
   * 用于合并数据到累加器的map中
   * 存在的累加，不存在的直接添加
   *
   * @param kv
   * 累加值的key和value
   */
  private[this] def mergeTable(kv: (String, String, Long)): Unit = {
    if (kv != null && StringUtils.isNotBlank(kv._1) && kv._2 != null) {
      val value = if (this.timerCountTable.contains(kv._1, kv._2)) this.timerCountTable.get(kv._1, kv._2) else 0L
      this.timerCountTable.put(kv._1, kv._2, kv._3 + value)
      this.clear
    }
  }

  /**
   * 用于合并executor端的map到driver端
   *
   * @param other
   * executor端的map
   */
  override def merge(other: AccumulatorV2[(String, Long, String), HashBasedTable[String, String, Long]]): Unit = {
    val otherTable = other.value
    if (otherTable != null && !otherTable.isEmpty) {
      otherTable.cellSet().foreach(timer => {
        this.mergeTable(timer.getRowKey, timer.getColumnKey, timer.getValue)
      })
    }
  }

  /**
   * 用于driver端获取累加器（map）中的值
   *
   * @return
   * 累加器中的值
   */
  override def value: HashBasedTable[String, String, Long] = this.timerCountTable

  /**
   * 当累积量超过maxTimerSize所设定的值时清理过期的数据
   */
  private[this] def clear: Unit = {
    val currentDate = new Date
    if (this.timerCountTable.size() >= FireFrameworkConf.maxTimerSize && DateFormatUtils.betweenHours(currentDate, lastClearTime) >= FireFrameworkConf.maxTimerHour) {
      val criticalTime = DateFormatUtils.addHours(currentDate, -Math.abs(FireFrameworkConf.maxTimerHour))

      val timeOutSet = new mutable.HashSet[String]()
      this.timerCountTable.rowMap().foreach(kmap => {
        kmap._2.filter(_ != null).foreach(kv => {
          if (kv._1.compareTo(criticalTime) <= 0 && StringUtils.isNotBlank(kmap._1) && StringUtils.isNotBlank(kv._1)) {
            timeOutSet += kmap._1 + "#" + kv._1
          }
        })
      })

      timeOutSet.filter(StringUtils.isNotBlank).map(t => (t.split("#"))).foreach(kv => {
        this.timerCountTable.remove(kv(0), kv(1))
      })
      this.lastClearTime = currentDate
    }
  }
}
