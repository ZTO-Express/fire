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

package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector.RocketMQ
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import org.apache.flink.api.scala._

/**
 * Flink流式计算任务消费rocketmq
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-5-13 14:26:24
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Checkpoint(60)
@Config("default.parallelism=2")
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire", tag = "*", startingOffset = "latest")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object RocketTest extends FlinkStreaming {

  override def process: Unit = {
    // 1. createRocketMqPullStreamWithTag()返回的是三元组，分别是：(tag, key, value)
    this.fire.createRocketMqPullStreamWithTag().setParallelism(1).map(t => {
      this.logInfo("消息：" + t._3)
      t._3
    }).print()

    // 2. createRocketMqPullStreamWithKey()返回的是二元组，分别是：(key, value)
    // this.fire.createRocketMqPullStreamWithKey().map(t => t._2).print()

    // 3. createRocketMqPullStream()返回的是消息体
    // this.fire.createRocketMqPullStream()

    // 从另一个rocketmq中消费数据
  }
}
