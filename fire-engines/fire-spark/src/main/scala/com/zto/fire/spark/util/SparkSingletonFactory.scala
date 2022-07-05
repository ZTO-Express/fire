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

package com.zto.fire.spark.util

import com.zto.fire.core.util.SingletonFactory
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.conf.FireHBaseConf
import com.zto.fire.spark.connector.HBaseBulkConnector
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
 * 单例工厂，用于创建单例的对象
 * Created by ChengLong on 2018-04-25.
 */
object SparkSingletonFactory extends SingletonFactory {
  private[this] var sparkSession: SparkSession = _
  private[this] var streamingContext: StreamingContext = _
  @transient private[this] var hbaseContext: HBaseBulkConnector = _

  /**
   * 获取SparkSession实例
   *
   * @return
   * SparkSession实例
   */
  def getSparkSession: SparkSession = this.synchronized {
    this.sparkSession
  }

  /**
   * SparkSession赋值
   */
  private[fire] def setSparkSession(sparkSession: SparkSession): Unit = this.synchronized {
    require(sparkSession != null, "SparkSession实例不能为空")
    this.sparkSession = sparkSession
  }

  /**
   * 设置StreamingContext
   * 允许重复赋值，兼容热重启导致的StreamingContext重新被创建
   */
  private[fire] def setStreamingContext(ssc: StreamingContext): Unit = this.synchronized {
    require(ssc != null, "StreamingContext实例不能为空")
    this.streamingContext = ssc
  }

  /**
   * 获取StreamingContext实例
   */
  def getStreamingContext: StreamingContext = this.synchronized {
    assert(this.streamingContext != null, "StreamingContext还没初始化，请稍后再试")
    this.streamingContext
  }


  /**
   * 获取单例的HBaseContext对象
   *
   * @param sparkContext
   * SparkContext实例
   * @return
   */
  def getHBaseContextInstance(sparkContext: SparkContext, keyNum: Int = 1): HBaseBulkConnector = this.synchronized {
    if (this.hbaseContext == null && StringUtils.isNotBlank(FireHBaseConf.hbaseCluster())) {
      this.hbaseContext = new HBaseBulkConnector(sparkContext, HBaseConnector.getConfiguration(keyNum))
    }
    this.hbaseContext
  }

}
