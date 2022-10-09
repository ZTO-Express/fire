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
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * 基于Fire进行Flink Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |# 支持Flink调优参数、Fire框架参数、用户自定义参数等
    |state.checkpoints.num-retained=30
    |state.checkpoints.dir=hdfs:///user/flink/checkpoint
    |""")
@Hive("thrift://localhost:9083") // 配置连接到指定的hive
@Streaming(interval = 100, unaligned = true, parallelism = 4) // 100s做一次checkpoint，开启非对齐checkpoint
@Kafka(brokers = "localhost:9092", topics = "fire", groupId = "fire")
object FlinkDemo extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    val dstream = this.fire.createKafkaDirectStream() 	// 使用api的方式消费kafka
    sql("""create table statement ...""")
    sql("""insert into statement ...""")
  }
}

