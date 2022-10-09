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

package com.zto.fire.spark.sync

import com.zto.fire.common.bean.lineage.Lineage
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.{DatasourceDesc, SQLLineageManager}
import com.zto.fire.core.sync.LineageAccumulatorManager
import com.zto.fire.predef._
import com.zto.fire.spark.acc.AccumulatorManager

/**
 * 用于将各个executor端数据收集到driver端
 *
 * @author ChengLong 2022-08-24 14:31:08
 * @since 2.3.2
 */
object SparkLineageAccumulatorManager extends LineageAccumulatorManager {

  /**
   * 将血缘信息放到累加器中
   */
  override def add(lineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
    AccumulatorManager.addLineage(lineage)
  }

  /**
   * 累加Long类型数据
   */
  override def add(value: Long): Unit = AccumulatorManager.addCounter(value)

  /**
   * 获取收集到的血缘消息
   */
  override def getValue: Lineage = {
    new Lineage(AccumulatorManager.getLineage, SQLLineageManager.getSQLLineage)
  }
}
