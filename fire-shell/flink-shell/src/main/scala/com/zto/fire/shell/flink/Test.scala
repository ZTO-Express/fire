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

package com.zto.fire.shell.flink

import com.zto.fire.common.anno.Config
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 基于Fire进行Flink Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    | #注释信息
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |flink.stream.checkpoint.interval=60000
    |flink.state.choose.disk.policy=round_robin
    |fire.analysis.arthas.enable=false
    |fire.log.level.conf.org.apache.flink=warn
    |fire.analysis.arthas.container.enable=false
    |fire.rest.filter.enable=true
    |""")
object Test extends BaseFlinkStreaming {

  def getFire: StreamExecutionEnvironment = this.fire

  def getStreamTableEnv: StreamTableEnvironment = this.steamTableEnv
}