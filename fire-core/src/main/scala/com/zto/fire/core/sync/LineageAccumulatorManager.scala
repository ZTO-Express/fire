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

package com.zto.fire.core.sync

import com.zto.fire.common.bean.lineage.Lineage
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.DatasourceDesc
import com.zto.fire.predef._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 用于将各个container端数据收集到master端
 *
 * @author ChengLong 2022-08-24 14:16:08
 * @since 2.3.2
 */
trait LineageAccumulatorManager extends SyncManager {
  private lazy val accumulator = new ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]()
  private lazy val longCounter = new AtomicLong()

  /**
   * 将消息放到累加器中
   */
  def add(lineage: ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
    if (FireFrameworkConf.accEnable) this.accumulator.putAll(lineage)
  }

  /**
   * 累加Long类型数据
   */
  def add(value: Long): Unit = this.longCounter.addAndGet(value)

  /**
   * 获取收集到的消息
   */
  def getValue: Lineage
}
