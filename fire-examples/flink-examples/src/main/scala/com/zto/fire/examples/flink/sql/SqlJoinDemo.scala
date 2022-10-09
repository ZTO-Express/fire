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

package com.zto.fire.examples.flink.sql

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * 本示例代码用于演示以下使用场景：
 * 1. 演示如何通过配置方式替换sql中with的options选项：'datasource'='alias'
 * 2. 演示如何一次性执行多条sql语句：以分号分割
 *
 * @author ChengLong 2022-08-22 17:18:49
 * @since 2.3.1
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(interval = 100, unaligned = true, parallelism = 2)
object SqlJoinDemo extends FlinkStreaming {

  /**
   * 建议将sql的options定义到[resources/common.properties]中，可被所有任务所加载
   * 也可以定义到实时平台中，fire框架在启动时通过接口调用获取，当然，也可以通过@Config注解定义
   */
  @Process
  def executeSql: Unit = {
    sql(
      s"""
         |-- 1. 定义kafka connector
         |CREATE TABLE t_kafka_fire (
         |  `id` BIGINT,
         |  `name` STRING,
         |  `age` INT,
         |  `createTime` TIMESTAMP(3),
         |  `length` double
         |) WITH (
         |  'datasource' = 'kafka_test',  -- 数据源别名定义在common.properties中，也可通过@Config注解定义
         |  'scan.startup.mode' = 'earliest-offset',
         |  'format' = 'json'
         |);
         |
         |-- 2. 定义kafka connector（另一个topic，用于双流join）
         |CREATE TABLE t_kafka_fire2 (
         |  `id` BIGINT,
         |  `name` STRING,
         |  `age` INT,
         |  `createTime` TIMESTAMP(3),
         |  `length` double
         |) WITH (
         |  'datasource' = 'kafka_test2', -- 数据源别名定义在common.properties中，也可通过@Config注解定义
         |  'scan.startup.mode' = 'earliest-offset',
         |  'format' = 'json'
         |);
         |
         |-- 3. 定义mysql目标表
         |CREATE TABLE t_flink_agg (
         |  `id` BIGINT,
         |  `name` STRING,
         |  `ds` STRING,
         |  `count_value` BIGINT,
         |  PRIMARY KEY (id) NOT ENFORCED
         |) WITH (
         |   'datasource' = 'jdbc_test',  -- 数据源别名定义在common.properties中，也可通过@Config注解定义
         |   'table-name' = 't_flink_agg',
         |   'sink.buffer-flush.interval' = '10s',
         |   'sink.buffer-flush.max-rows' = '3',
         |   'sink.max-retries' = '3'
         |);
         |
         |-- 将双流join的数据写入到mysql表中
         |insert into t_flink_agg(id, name, ds, count_value)
         |select
         |  k1.id,
         |  k2.name,
         |  DATE_FORMAT(k1.createTime, 'yyyyMMdd') as ds,
         |  count(1)
         |from t_kafka_fire k1 left join t_kafka_fire2 k2 on k1.id=k2.id
         |group by k1.id, k2.name, DATE_FORMAT(k1.createTime, 'yyyyMMdd')
         |""".stripMargin).show()
  }
}