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

import com.zto.fire.predef._
import com.zto.fire.common.util.OSUtils
import com.zto.fire.core.sync.SyncManager
import com.zto.fire.spark.util.SparkSingletonFactory

import java.util.concurrent.atomic.AtomicInteger

/**
 * Spark分布式数据同步管理器，用于将数据从Driver端同步至每一个executor端
 *
 * @author ChengLong 2021-11-3 14:14:51
 * @since 2.2.0
 */
object DistributeSyncManager extends SyncManager {
  private[this] lazy val initExecutors = new AtomicInteger(0)
  private[this] lazy val spark = SparkSingletonFactory.getSparkSession
  private[this] lazy val sc = this.spark.sparkContext

  /**
   * 获取当前任务的executor数
   */
  private[fire] def getInitExecutors: Int = {
    if (this.initExecutors.get() == 0) this.initExecutors.set(this.sc.getConf.get("spark.executor.instances", if (OSUtils.isLinux) "1000" else "10").toInt)
    this.initExecutors.get()
  }

  /**
   * 根据当前executor数量，创建10倍于executor以上的数据集，并利用foreachPartition将给定的逻辑发送到每一个executor端执行
   *
   * @param fun
   * 在每一个executor端执行fun逻辑
   * @param isAsync
   * 是否以异步的方式执行
   */
  def sync(fun: => Unit, isAsync: Boolean = true): Unit = {
    if (isEmpty(SparkSingletonFactory.getSparkSession)) return
    val executorNum = this.getInitExecutors
    val rdd = this.sc.parallelize(1 to executorNum * 10, executorNum * 3)
    if (isAsync) {
      rdd.foreachPartitionAsync(_ => {
        tryWithLog(fun)(this.logger, tryLog = "Synchronizing data to the executor is complete.", catchLog = "Synchronizing data to the executor is failed.")
      })
    } else {
      rdd.foreachPartition(_ => {
        tryWithLog(fun)(this.logger, tryLog = "Synchronizing data to the executor is complete.", catchLog = "Synchronizing data to the executor is failed.")
      })
    }
  }
}
