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

package com.zto.fire.spark.conf

import com.zto.fire.common.conf.FireFrameworkConf.FIRE_JOB_AUTO_START
import com.zto.fire.core.conf.AnnoManager
import com.zto.fire.spark.anno.{Streaming, StreamingDuration}

/**
 * 注解管理器，用于将主键中的配置信息映射为键值对信息
 *
 * @author ChengLong 2022-04-26 11:19:00
 * @since 2.2.2
 */
private[fire] class SparkAnnoManager extends AnnoManager {

  /**
   * 将@StreamingDuration中配置的信息映射为键值对形式
   * @param StreamingDuration
   * StreamingDuration注解实例
   */
  def mapStreamingDuration(streaming: StreamingDuration): Unit = {
    this.put(FireSparkConf.SPARK_STREAMING_BATCH_DURATION, streaming.value())
    this.put(FireSparkConf.SPARK_STREAMING_BATCH_DURATION, streaming.interval())
    this.put("spark.streaming.receiver.writeAheadLog.enable", streaming.checkpoint())
  }

  /**
   * 将@Streaming中配置的信息映射为键值对形式
   * @param Streaming
   * Streaming注解实例
   */
  def mapStreaming(streaming: Streaming): Unit = {
    this.put(FireSparkConf.SPARK_STREAMING_BATCH_DURATION, streaming.value())
    this.put(FireSparkConf.SPARK_STREAMING_BATCH_DURATION, streaming.interval())
    this.put("spark.streaming.receiver.writeAheadLog.enable", streaming.checkpoint())
    this.put("spark.streaming.backpressure.enabled", streaming.backpressure())
    this.put("spark.streaming.concurrentJobs", streaming.concurrent())
    this.put("spark.streaming.stopGracefullyOnShutdown", streaming.stopGracefullyOnShutdown())
    this.put("spark.streaming.kafka.maxRatePerPartition", streaming.maxRatePerPartition())
    this.put("spark.streaming.backpressure.initialRate", streaming.backpressureInitialRate())
    this.put("spark.rocket.pull.max.speed.per.partition", streaming.maxRatePerPartition())
    this.put(FIRE_JOB_AUTO_START, streaming.autoStart())
  }

  /**
   * 用于注册需要映射配置信息的自定义主键
   */
  override protected[fire] def register: Unit = {
    AnnoManager.registerAnnoSet.add(classOf[StreamingDuration])
    AnnoManager.registerAnnoSet.add(classOf[Streaming])
  }
}
