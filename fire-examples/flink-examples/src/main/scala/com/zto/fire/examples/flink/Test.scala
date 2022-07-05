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

package com.zto.fire.examples.flink


import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.anno.Checkpoint

/**
 * 基于Fire进行Flink Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |# flink调优参数、fire框架参数、用户自定义参数
    |flink.kafka.force.overwrite.stateOffset.enable=true
    |# 是否在开启checkpoint的情况下强制开启周期性offset提交
    |flink.kafka.force.autoCommit.enable=true
    |""")
@HBase("test")  // 配置连接到指定的Hbase
@Hive("test") // 配置连接到指定的hive
@Checkpoint(interval = 100, unaligned = true) // 100s做一次checkpoint，开启非对齐checkpoint
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka2(brokers = "bigdata_test", topics = "fire2", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object Test extends BaseFlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print("fire1==> ")
    this.fire.start
  }
}