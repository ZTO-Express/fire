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

package com.zto.fire.examples.spark.acc

import com.zto.fire._
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.{DateFormatUtils, PropUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

import java.util.concurrent.TimeUnit


/**
 * 用于演示与测试Fire框架内置的累加器
 *
 * @author ChengLong 2019年9月10日 09:50:16
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(10)
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object FireAccTest extends SparkStreaming {
  val key = "fire.partitions"

  override def process: Unit = {
    if (this.args != null) {
      this.args.foreach(arg => println(arg + " "))
    }
    val dstream = this.fire.createKafkaDirectStream()
    dstream.foreachRDD(rdd => {
      rdd.coalesce(this.conf.getInt(key, 10)).foreachPartition(t => {
        println("conf=" + this.conf.getInt(key, 10) + " PropUtils=" + PropUtils.getString(key))
        // 单值累加器
        this.acc.addCounter(1)
        // 多值累加器，根据key的不同分别进行数据的累加
        this.acc.addMultiCounter("multiCounter", 1)
        this.acc.addMultiCounter("partitions", 1)
        // 多时间维度累加器，比多值累加器多了一个时间维度，如：hbaseWriter  2019-09-10 11:00:00  10
        this.acc.addMultiTimer("multiTimer", 1)
      })
    })

    // 定时打印fire内置累加器中的值
    ThreadUtils.schedule(this.printAcc, 0, 10, true, TimeUnit.MINUTES)
  }

  /**
   * 打印累加器中的值
   */
  def printAcc: Unit = {
    println(s"===============${DateFormatUtils.formatCurrentDateTime()}=============")
    this.acc.getMultiTimer.cellSet().foreach(t => println(s"key：" + t.getRowKey + " 时间：" + t.getColumnKey + " " + t.getValue + "条"))

    println("单值：" + this.acc.getCounter)
    this.acc.getMultiCounter.foreach(t => {
      println("多值：key=" + t._1 + " value=" + t._2)
    })
    val size = this.acc.getMultiTimer.cellSet().size()

    println(s"======multiTimer.size=${size}==log.size=${this.acc.getLog.size()}======")
  }

  @Scheduled(fixedInterval = 60 * 1000, scope = "all")
  def loadTable: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}=================== 每分钟执行loadTable ===================")
  }

  @Scheduled(cron = "0 0 * * * ?")
  def loadTable2: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}=================== 每小时执行loadTable2 ===================")
  }

  @Scheduled(cron = "0 0 9 * * ?")
  def loadTable3: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}=================== 每天9点执行loadTable3 ===================")
  }
}
