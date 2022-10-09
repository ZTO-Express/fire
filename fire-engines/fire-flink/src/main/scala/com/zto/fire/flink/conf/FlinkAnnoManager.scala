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

package com.zto.fire.flink.conf

import com.zto.fire.common.conf.FireFrameworkConf.FIRE_JOB_AUTO_START
import com.zto.fire.common.util.PropUtils
import com.zto.fire.core.conf.AnnoManager
import com.zto.fire.flink.anno.{Checkpoint, FlinkConf, Streaming}
import com.zto.fire.flink.conf.FireFlinkConf._

/**
 * 注解管理器，用于将主键中的配置信息映射为键值对信息
 *
 * @author ChengLong 2022-04-26 11:19:00
 * @since 2.2.2
 */
private[fire] class FlinkAnnoManager extends AnnoManager {

  /**
   * 将@Streaming中配置的信息映射为键值对形式
   * @param streaming
   * Streaming注解实例
   */
  def mapStreaming(streaming: Streaming): Unit = {
    /**
     * 将时间单位由s转为ms
     */
    def unitConversion(value: Int): Int = if (value > 0) value * 1000 else -1

    this.put(FLINK_STREAM_CHECKPOINT_INTERVAL, unitConversion(streaming.value()))
    this.put(FLINK_STREAM_CHECKPOINT_INTERVAL, unitConversion(streaming.interval()))
    this.put(FLINK_STREAM_CHECKPOINT_TIMEOUT, unitConversion(streaming.timeout()))
    this.put(FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, unitConversion(streaming.pauseBetween()))
    this.put(FLINK_STREAM_CHECKPOINT_UNALIGNED, streaming.unaligned())
    this.put(FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, streaming.concurrent())
    this.put(FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, streaming.failureNumber())
    this.put(FLINK_STREAM_CHECKPOINT_MODE, streaming.mode())
    this.put(streamCheckpointExternalized, streaming.cleanup())
    this.put(FIRE_JOB_AUTO_START, streaming.autoStart())
    this.put(FLINK_DEFAULT_PARALLELISM, streaming.parallelism())
    this.put(OPERATOR_CHAINING_ENABLE, streaming.disableOperatorChaining())
    this.put(FLINK_STATE_TTL_DAYS, streaming.stateTTL())
  }

  /**
   * 将@Checkpoint中配置的信息映射为键值对形式
   * @param checkpoint
   * Checkpoint注解实例
   */
  def mapCheckpoint(checkpoint: Checkpoint): Unit = {
    /**
     * 将时间单位由s转为ms
     */
    def unitConversion(value: Int): Int = if (value > 0) value * 1000 else -1

    this.put(FLINK_STREAM_CHECKPOINT_INTERVAL, unitConversion(checkpoint.value()))
    this.put(FLINK_STREAM_CHECKPOINT_INTERVAL, unitConversion(checkpoint.interval()))
    this.put(FLINK_STREAM_CHECKPOINT_TIMEOUT, unitConversion(checkpoint.timeout()))
    this.put(FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, unitConversion(checkpoint.pauseBetween()))
    this.put(FLINK_STREAM_CHECKPOINT_UNALIGNED, checkpoint.unaligned())
    this.put(FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, checkpoint.concurrent())
    this.put(FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, checkpoint.failureNumber())
    this.put(FLINK_STREAM_CHECKPOINT_MODE, checkpoint.mode())
    this.put(streamCheckpointExternalized, checkpoint.cleanup())
  }

  /**
   * 将@FlinkConf中配置的信息映射为键值对形式
   */
  def mapFlinkConf(flinkConf: FlinkConf): Unit = {
    val valueConf = PropUtils.parseTextConfig(flinkConf.value())
    valueConf.foreach(kv => this.props.put(kv._1, kv._2))
    flinkConf.props().foreach(prop => {
      val conf = prop.split("=", 2)
      if (conf != null && conf.length == 2) {
        this.props.put(conf(0), conf(1))
      }
    })
  }

  /**
   * 用于注册需要映射配置信息的自定义主键
   */
  override protected[fire] def register: Unit = {
    AnnoManager.registerAnnoSet.add(classOf[FlinkConf])
    AnnoManager.registerAnnoSet.add(classOf[Streaming])
    AnnoManager.registerAnnoSet.add(classOf[Checkpoint])
  }
}
