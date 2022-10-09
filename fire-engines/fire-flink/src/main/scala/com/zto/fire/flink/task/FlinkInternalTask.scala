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

package com.zto.fire.flink.task

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.{JSONUtils, MQProducer}
import com.zto.fire.core.task.FireInternalTask
import com.zto.fire.flink.BaseFlink
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager

/**
 * 定时任务调度器，用于定时执行Flink框架内部指定的任务
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-14 11:04
 */
private[fire] class FlinkInternalTask(baseFlink: BaseFlink) extends FireInternalTask(baseFlink) {

  /**
   * 实时血缘发送定时任务，定时将血缘信息发送到kafka中
   */
  @Scheduled(fixedInterval = 60000, initialDelay = 10000, repeatCount = 360)
  override def lineage: Unit = {
    sendLineage
    this.registerLineageHook(sendLineage)

    def sendLineage: Unit = {
      if (FireFrameworkConf.lineageEnable && FireFrameworkConf.lineageSendMqEnable) {
        MQProducer.sendKafka(FireFrameworkConf.lineageMQUrl, FireFrameworkConf.lineageTopic, JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue))
      }
    }
  }
}
