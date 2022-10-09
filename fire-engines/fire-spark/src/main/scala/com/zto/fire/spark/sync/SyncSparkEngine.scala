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

import com.zto.fire._
import com.zto.fire.common.bean.lineage.Lineage
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.{DatasourceDesc, Logging, PropUtils}
import com.zto.fire.core.sync.SyncEngineConf
import com.zto.fire.spark.acc.AccumulatorManager
import com.zto.fire.spark.util.SparkUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

/**
 * 获取Spark引擎的所有配置信息
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-02 10:57
 */
private[fire] class SyncSparkEngine extends SyncEngineConf {

  /**
   * 获取引擎的所有配置信息
   */
  override def syncEngineConf: Map[String, String] = {
    if (SparkUtils.isExecutor) {
      SparkEnv.get.conf.getAll.toMap
    } else {
      Map.empty[String, String]
    }
  }

  /**
   * 在master端获取系统累加器中的数据
   */
  override def syncLineage: Lineage = {
    SparkLineageAccumulatorManager.getValue
  }

  /**
   * 同步引擎各个container的信息到累加器中
   */
  override def collect: Unit = {
    if (SparkUtils.isDriver && isCollect.compareAndSet(false, true)) AccumulatorManager.collectLineage
  }
}

object SyncSparkEngine extends Logging {
  // 用于广播spark配置信息
  private[fire] var broadcastConf: Broadcast[SparkConf] = _

  /**
   * 将最新的配置信息以广播的方式同步给每一个executor
   */
  private[fire] def syncDynamicConf(sc: SparkContext, conf: SparkConf): Unit = {
    if (sc != null && conf != null && FireFrameworkConf.dynamicConf) {
      val broadcastConf = sc.broadcast(conf)
      this.broadcastConf = broadcastConf
      DistributeSyncManager.sync({
        this.broadcastConf = broadcastConf
        this.broadcastConf.value.getAll.foreach(kv => {
          PropUtils.setProperty(kv._1, kv._2)
        })
        this.logger.info("The Executor side configuration has been reloaded.")
      })
      this.logger.info("The Driver side configuration has been reloaded.")
    }
  }
}