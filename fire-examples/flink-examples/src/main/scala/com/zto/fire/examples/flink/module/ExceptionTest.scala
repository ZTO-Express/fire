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
import com.zto.fire.common.util.{JSONUtils, ShutdownHookManager}
import com.zto.fire.core.anno.connector._
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.flink.sql.RocketMQConnectorTest.sql
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala._

@Streaming(interval = 10, unaligned = true, parallelism = 2) // 100s做一次checkpoint，开启非对齐checkpoint
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object ExceptionTest extends FlinkStreaming {

  override def process: Unit = {
    this.testSqlException
    // this.testApiException
  }

  /**
   * 测试SQL异常捕获
   */
  def testSqlException: Unit = {
    sql("""
          |CREATE table source (
          |  id int,
          |  name string,
          |  age int,
          |  length double,
          |  data DECIMAL(10, 5)
          |) with (
          | 'connector'='fire-rocketmq',
          | 'format'='json',
          | 'rocket.brokers.name'='bigdata_test',
          | 'rocket.topics'='fire',
          | 'rocket.group.id'='fire',
          | 'rocket.consumer.tag'='*'
          |);
          |
          |CREATE table sink (
          |  id int,
          |  name string,
          |  age int,
          |  length double,
          |  data DECIMAL(10, 5)
          |) with (
          | 'connector'='fire-rocketmq',
          | 'format'='json',
          | 'rocket.brokers.name'='bigdata_test',
          | 'rocket.topics'='fire2',
          | 'rocket.consumer.tag'='*',
          | 'rocket.sink.parallelism'='1'
          |);
          |
          |insert into select * from source;
          |""".stripMargin)
  }

  /**
   * 测试API的异常捕获
   */
  def testApiException: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.map(t => {
      val student = JSONUtils.parseObject[Student](t)
      if (student.getId % 2 != 0) {
        val a = 1 / 0
      }
      t
    }).print()
  }
}