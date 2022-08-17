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

package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.Kafka
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.util.Collector

/**
 * 基于Fire进行Flink Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |fire.conf.test=java
    |""")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Checkpoint(interval = 10, concurrent = 1, pauseBetween = 60, timeout = 60)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object ConfigCenterTest extends FlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().filter(json => JSONUtils.isJson(json)).map(json => JSONUtils.parseObject[Student](json)).setParallelism(2)
    val value: KeyedStream[Student, JLong] = dstream.keyBy(t => t.getId)
    this.printConf

    value.process(new KeyedProcessFunction[JLong, Student, String]() {

      override def processElement(value: Student, ctx: KeyedProcessFunction[_root_.com.zto.fire.JLong, Student, String]#Context, out: Collector[String]): Unit = {
        printConf
        val state = this.getState[String]("sum")
        state.update(state.value() + JSONUtils.toJSONString(value))

        out.collect(value.getName)
      }

    }).print("name")
  }

  /**
   * 配置信息打印
   *
   *  ================================
   *  fire.thread.pool.size=6
   *  fire.thread.pool.schedule.size=5
   *  fire.acc.timer.max.size=30
   *  fire.acc.log.max.size=22
   *  fire.jdbc.query.partitions=13
   *  fire.conf.test=flink
   *  ================================
   */
  def printConf: Unit = {
    println("================================")
    println("fire.thread.pool.size=" + this.conf.getInt("fire.thread.pool.size", -1))
    println("fire.thread.pool.schedule.size=" + this.conf.getInt("fire.thread.pool.schedule.size", -1))
    println("fire.acc.timer.max.size=" + this.conf.getInt("fire.acc.timer.max.size", -1))
    println("fire.acc.log.max.size=" + this.conf.getInt("fire.acc.log.max.size", -1))
    println("fire.jdbc.query.partitions=" + this.conf.getInt("fire.jdbc.query.partitions", -1))
    println("fire.conf.test=" + this.conf.getString("fire.conf.test"))
    println("================================")
  }
}