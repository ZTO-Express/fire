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
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * RocketMQ connector
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |# flink所支持的参数
    |state.checkpoints.num-retained      =       3
    |state.backend.incremental           =       true
    |state.backend.rocksdb.files.open    =       5000
    |
    |# sql中with表达，配置方法是以flink.sql.with开头，跟上connector的key，以数字结尾，用于区分不同的connector
    |flink.sql.with.connector=fire-rocketmq
    |flink.sql.with.format=json
    |flink.sql.with.rocket.brokers.name=bigdata_test
    |flink.sql.with.rocket.topics=fire
    |flink.sql.with.rocket.group.id=fire
    |flink.sql.with.rocket.consumer.tag=*
    |
    |flink.sql.log.enable=true
    |flink.default.parallelism=2
    |flink.stream.checkpoint.interval=60000
    |
    |flink.sql.with.connector2=fire-rocketmq
    |flink.sql.with.format2=json
    |flink.sql.with.rocket.brokers.name2=bigdata_test
    |flink.sql.with.rocket.topics2=fire2
    |flink.sql.with.rocket.group.id2=fire2
    |flink.sql.with.rocket.starting.offsets=latest
    |flink.sql.with.rocket.consumer.tag2=*
    |
    |flink.sql.with.connector3=fire-rocketmq
    |flink.sql.with.format3=json
    |flink.sql.with.rocket.brokers.name3=bigdata_test
    |flink.sql.with.rocket.topics3=fire2
    |flink.sql.with.rocket.consumer.tag3=*
    |flink.sql.with.rocket.sink.parallelism3=1
    |""")
object RocketMQConnectorTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val test = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    println("path=" + test)
    this.fire.sql("""
                    |CREATE table source (
                    |  id bigint,
                    |  name string,
                    |  age int,
                    |  length double,
                    |  data DECIMAL(10, 5)
                    |)
                    |""".stripMargin, keyNum = 2)

    this.fire.sql("""
                    |CREATE table sink (
                    |  id bigint,
                    |  name string,
                    |  age int,
                    |  length double,
                    |  data DECIMAL(10, 5)
                    |)
                    |""".stripMargin, keyNum = 3)
    this.fire.sql(
      """
        |insert into sink select * from source
        |""".stripMargin)

    this.fire.sql(
      """
        |select * from source
        |""".stripMargin).print()
  }
}
