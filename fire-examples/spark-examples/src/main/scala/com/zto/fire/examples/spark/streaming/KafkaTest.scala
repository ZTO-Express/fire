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

package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.anno.connector.{Kafka, Kafka2, Kafka3}
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
 * kafka json解析
 *
 * @author ChengLong 2019-6-26 16:52:58
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(interval = 10)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka2(brokers = "bigdata_test", topics = "fire2", groupId = "fire")
@Kafka3(brokers = "bigdata_test", topics = "fire3", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object KafkaTest extends SparkStreaming {

  // 每天凌晨4点01将锁标志设置为false，这样下一个批次就可以先更新维表再执行sql
  @Scheduled(cron = "0 1 4 * * ?")
  def updateTableJob: Unit = this.lock.compareAndSet(true, false)

  // 用于缓存变更过的维表，只有当定时任务将标记设置为可更新时才会真正拉取最新的表
  def cacheTable: Unit = {
    // 加载完成维表以后上锁
    if (this.lock.compareAndSet(false, true)) {
      // cache维表逻辑
    }
  }

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    // 使用至少一次的算子语义，支持在rdd处理失败时自动重试，并且在处理成功后会主动提交offset
    dstream.foreachRDDAtLeastOnce(rdd => {
      // 更新并缓存维表动作，具体要根据锁的标记判断是否执行
      this.cacheTable

      // 一、将json解析并注册为临时表，默认不cache临时表
      rdd.kafkaJson2Table("test", cacheTable = true)
      // toLowerDF表示将大写的字段转为小写
      sql("select * from test").toLowerDF.show(1, false)
      /*sql("select after.* from test").toLowerDF.show(1, false)
      sql("select after.* from test where after.order_type=1").toLowerDF.show(1, false)*/

      // 二、直接将json按指定的schema解析（只解析after），fieldNameUpper=true表示按大写方式解析，并自动转为小写
      // rdd.kafkaJson2DF(classOf[OrderCommon], fieldNameUpper = true).show(1, false)
      // 递归解析所有指定的字段，包括before、table、offset等字段
      // rdd.kafkaJson2DF(classOf[OrderCommon], parseAll = true, fieldNameUpper = true, isMySQL = false).show(1, false)

      this.fire.uncache("test")
    })

    val dstream2 = this.fire.createKafkaDirectStream(keyNum = 2)
    dstream2.print(1)
    val dstream3 = this.fire.createKafkaDirectStream(keyNum = 3)
    dstream3.count().foreachRDD(rdd => {
      println("count=" + rdd.count())
    })
    dstream3.print(1)
  }

  @Scheduled(fixedInterval = 60 * 1000, scope = "all")
  def loadTable: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}=================== 每分钟执行loadTable ===================")
    this.conf.settings.foreach(conf => println(conf._1 + " -> " + conf._2))
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
