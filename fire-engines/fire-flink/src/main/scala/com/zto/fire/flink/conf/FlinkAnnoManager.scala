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

import com.zto.fire.core.conf.AnnoManager
import com.zto.fire.flink.anno.Checkpoint
import com.zto.fire.flink.conf.FireFlinkConf._

/**
 * 注解管理器，用于将主键中的配置信息映射为键值对信息
 *
 * @author ChengLong 2022-04-26 11:19:00
 * @since 2.2.2
 */
private[fire] class FlinkAnnoManager extends AnnoManager {

  /**
   * 将@Checkpoint中配置的信息映射为键值对形式
   * @param checkpoint
   * Checkpoint注解实例
   */
  def mapCheckpoint(checkpoint: Checkpoint): Unit = {
    this.put(FLINK_STREAM_CHECKPOINT_INTERVAL, checkpoint.value())
    this.put(FLINK_STREAM_CHECKPOINT_INTERVAL, checkpoint.interval())
    this.put(FLINK_STREAM_CHECKPOINT_TIMEOUT, checkpoint.timeout())
    this.put(FLINK_STREAM_CHECKPOINT_UNALIGNED, checkpoint.unaligned())
    this.put(FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, checkpoint.concurrent())
    this.put(FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, checkpoint.pauseBetween())
    this.put(FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, checkpoint.failureNumber())
    this.put(FLINK_STREAM_CHECKPOINT_MODE, checkpoint.mode())
    this.put(streamCheckpointExternalized, checkpoint.cleanup())
  }

  /**
   * 用于注册需要映射配置信息的自定义主键
   */
  override protected[fire] def register: Unit = {
    this.registerAnnoSet.add(classOf[Checkpoint])
  }
}
