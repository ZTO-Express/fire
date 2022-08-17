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

package com.zto.fire.examples.spark.thread

import com.zto.fire._
import com.zto.fire.common.util.{DateFormatUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.Kafka
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
  * 在driver中启用线程池的示例
  * 1. 开启子线程执行一个任务
  * 2. 开启子线程执行周期性任务
  *
  * @contact Fire框架技术交流群（钉钉）：35373471
  */
@Streaming(interval = 10, checkpoint = false, concurrent = 2)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object ThreadTest extends SparkStreaming {

  override def main(args: Array[String]): Unit = {
    // 第二个参数为true表示开启checkPoint机制
    this.init(10L, false)
  }

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    // 第一次执行时延迟两分钟，每隔1分钟执行一次showSchema函数
    ThreadUtils.schedule(this.showSchema, 1, 1)
    // 以子线程方式执行print方法中的逻辑
    ThreadUtils.run(this.print)

    val dstream = this.fire.createKafkaDirectStream()
    dstream.foreachRDD(rdd => {
      println("count--> " + rdd.count())
    })
  }

  /**
    * 以子线程方式执行一次
    */
  def print: Unit = {
    println("==========子线程执行===========")
  }

  /**
    * 查看表结构信息
    */
  def showSchema: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}--------------> atFixRate <----------------")
    sql("use tmp")
    sql("show tables").show(false)
  }
}
