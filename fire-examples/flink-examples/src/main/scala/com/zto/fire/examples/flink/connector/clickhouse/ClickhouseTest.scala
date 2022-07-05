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

package com.zto.fire.examples.flink.connector.clickhouse

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.Kafka
import com.zto.fire.examples.bean.Student
import org.apache.flink.api.scala._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.anno.Checkpoint


/**
 * flink clickhouse connector
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Checkpoint(60)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object ClickhouseTest extends BaseFlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  override def process: Unit = {
    // this.fire.sql(DDL.createStudent("t_kafka", 10))
    val dstream = this.fire.createKafkaDirectStream().filter(JSONUtils.isJson(_)).map(JSONUtils.parseObject[Student](_))
    dstream.createOrReplaceTempView("t_kafka")
    this.fire.sql(
      """
        |CREATE TABLE t_user (
        |    `id` BIGINT,
        |    `name` STRING,
        |    `age` INT,
        |    `sex` STRING,
        |    `score` DECIMAL,
        |    `birthday` TIMESTAMP
        |) WITH (
        |    'connector' = 'clickhouse',
        |    'url' = 'jdbc:clickhouse://node01:8123,node02:8123,node03:8123',
        |    'database-name' = 'study',
        |    'username' = 'root',
        |    'password' = 'fire',
        |    'use-local' = 'true', -- 指定为true，当分布式表写入时写的是本地表
        |    'table-name' = 't_student',
        |    'sink.batch-size' = '10',
        |    'sink.flush-interval' = '3',
        |    'sink.max-retries' = '3'
        |)
        |""".stripMargin)

    this.fire.sql(
      """
        |insert into t_user
        |select
        |   id, name, age,
        |   case when sex then '男' else '女' end,
        |   cast(length as DECIMAL),
        |   cast(createTime as TIMESTAMP)
        |from t_kafka
        |""".stripMargin)

    this.fire.sql(
      """
        |select
        |   id, name, age,
        |   case when sex then '男' else '女' end,
        |   cast(length as DECIMAL),
        |   cast(createTime as TIMESTAMP)
        |from t_kafka
        |""".stripMargin).print()
  }
}