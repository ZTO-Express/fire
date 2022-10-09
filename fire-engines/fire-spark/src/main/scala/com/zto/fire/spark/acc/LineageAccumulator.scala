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

import com.zto.fire._
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.{DatasourceDesc, LineageManager, Logging}
import com.zto.fire.predef.JHashSet
import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

/**
  * Fire框架实时血缘累加器，用于采集实时任务用到的数据源信息、SQL血缘信息等
  * 支持：SQL、JDBC、Kafka、RocketMQ、HBase等组件的血缘信息解析与采集
  *
  * @author ChengLong 2022-08-29 09:21:48
  * @since 2.3.2
  */
private[fire] class LineageAccumulator extends AccumulatorV2[ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]], ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]] with Logging {
  // 用于存放字符串的队列
  private val lineageMap = new ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]()

  /**
    * 判断累加器是否为空
    */
  override def isZero: Boolean = this.lineageMap.isEmpty

  /**
    * 用于复制累加器
    */
  override def copy(): AccumulatorV2[ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]], ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]] = {
    val strAcc = new LineageAccumulator
    // strAcc.value.putAll(this.lineageMap)
    LineageManager.mergeLineageMap(strAcc.value, this.lineageMap)
    strAcc
  }

  /**
    * driver端执行有效，用于清空累加器
    */
  override def reset(): Unit = this.lineageMap.clear()

  /**
   * 将新的血缘信息添加到累加器中
   */
  override def add(v: ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
    if (FireFrameworkConf.accEnable && v.nonEmpty) {
      // this.lineageMap.putAll(v)
      LineageManager.mergeLineageMap(this.lineageMap, v)
    }
  }

  /**
    * executor端向driver端merge累加数据
    *
    * @param other
    * executor端累加结果
    */
  override def merge(other: AccumulatorV2[ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]], ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]]): Unit = {
    if (other != null && other.value.size() > 0) {
      this.lineageMap.putAll(other.value)
    }
  }

  /**
    * driver端获取累加器的值
    *
    * @return
    * 收集到的日志信息
    */
  override def value: ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = this.lineageMap
}
