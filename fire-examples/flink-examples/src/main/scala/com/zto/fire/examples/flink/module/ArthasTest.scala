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

package com.zto.fire.examples.flink.module

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
 * Flink Streaming与Arthas集成测试
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    | #注释信息
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |fire.analysis.arthas.enable=false
    |fire.log.level.conf.org.apache.flink=warn
    |fire.analysis.arthas.container.enable=false
    |fire.rest.filter.enable=true
    |""")
@Checkpoint(interval = 10, concurrent = 1, pauseBetween = 60, timeout = 60)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object ArthasTest extends FlinkStreaming {

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
        val state = this.getState[Long]("sum")
        state.update(state.value() + 1)
        println(s"当前key=${value.getId} sum=${state.value()}")
        out.collect(value.getName)
      }

    }).print("name")
  }

  def printConf: Unit = {
    println("================================")
    println("fire.thread.pool.size=" + this.conf.getInt("fire.thread.pool.size", -1))
    println("fire.thread.pool.schedule.size=" + this.conf.getInt("fire.thread.pool.schedule.size", -1))
    println("================================")
  }
}