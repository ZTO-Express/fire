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
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |fire.shutdown.auto.exit=true
    |""")
@Hive("test")
@Streaming(20) // spark streaming的批次时间
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object AtLeastOnceTest extends SparkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()

    // 至少一次的语义保证，处理成功自动提交offset，处理失败会重试指定次数，如果仍失败则任务退出
    dstream.foreachRDDAtLeastOnce(rdd => {
      val studentRDD = rdd.map(t => JSONUtils.parseObject[Student](t.value())).repartition(2)
      val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
      println("kafka.brokers.name=>" + this.conf.getString("kafka.brokers.name"))
      studentRDD.toDF().jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 1)
    })
  }
}
