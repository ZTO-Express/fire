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
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.{RocketMQ, RocketMQ2}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
 * 消费rocketmq中的数据
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(10)
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire", tag = "*")
@RocketMQ2(brokers = "bigdata_test", topics = "fire2", groupId = "fire2", tag = "*", startingOffset = "latest")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object RocketTest extends BaseSparkStreaming {
  override def process: Unit = {
    // 读取RocketMQ消息流
    val dStream = this.fire.createRocketMqPullStream()
    this.fire.createRocketMqPullStream(keyNum = 2).print()
    dStream.foreachRDDAtLeastOnce(rdd => {
      val studentRDD = rdd.map(message => new String(message.getBody)).map(t => JSONUtils.parseObject[Student](t)).repartition(2)
      val insertSql = s"INSERT INTO spark_test2(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
      println("rocket.brokers.name=>" + this.conf.getString("rocket.brokers.name"))
      studentRDD.toDF().jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 100)
    })(reTry = 5, exitOnFailure = true)
    this.fire.start()
  }
}
