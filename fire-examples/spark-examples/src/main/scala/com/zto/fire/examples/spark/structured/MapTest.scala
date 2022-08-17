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

package com.zto.fire.examples.spark.structured

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseStructuredStreaming
import org.apache.spark.sql.Encoders
import com.zto.fire.spark.util.SparkUtils

/**
 * 对结构化流执行map、mapPartition操作
 *
 * @author ChengLong 2020年1月3日 18:00:59
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Hive("batch")
@Kafka(brokers = "test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object MapTest extends BaseStructuredStreaming {

  override def process: Unit = {
    this.fire.loadKafkaParseJson()

    // 将字段转为与JavaBean对应的类型
    val sqlDF = sql("select cast(age as int), createTime, cast(length as decimal), name, rowKey, cast(sex as boolean) from kafka")

    // 执行map操作
    sqlDF.map(row => {
        // 执行任意的操作
        println("=========hello===========")
        // 将row转为JavaBean
        SparkUtils.sparkRowToBean(row, classOf[Student])
        // 指定Encoders，必须是具有schema的目标类型，map后的类型即为Encoders中要指定的类型。不支持对普通数值类型的map，必须是DateType的子类
      })(Encoders.bean(classOf[Student])).print()

    // mapPartition操作
    sqlDF.mapPartitions(it => SparkUtils.sparkRowToBean(it, classOf[Student]))(Encoders.bean(classOf[Student])).print()
  }
}
