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
import com.zto.fire.core.anno.lifecycle._
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * 用于演示通过flink sql读取mysql维表表
 *
 * @author ChengLong 2022-08-23 14:21:55
 * @since 2.0.0
 */
@Streaming(interval = 60, parallelism = 2)
object JdbcDimDemo extends FlinkStreaming {

  @Step1("创建mysql维表数据源")
  def dimTable: Unit = {
    sql(
      """
        |CREATE TABLE t_mysql_dim (
        |  `id` BIGINT,
        |  `name` STRING,
        |  `ds` STRING,
        |  `count_value` BIGINT,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |   'datasource' = 'jdbc_test',  -- 数据源别名定义在common.properties中，也可通过@Config注解定义
        |   'table-name' = 't_flink_agg',
        |   'lookup.cache.max-rows'='1000',
        |   'lookup.cache.ttl' = '1h',
        |   'lookup.max-retries' = '3'
        |);
        |""".stripMargin)
  }

  @Step2("创建kafka数据源")
  def kafkaTable: Unit = {
    this.fire.useDefaultCatalog
    sql(
      """
        |-- 1. 定义kafka connector
        |CREATE TABLE t_kafka_fire (
        |  `id` BIGINT,
        |  `name` STRING,
        |  `age` INT,
        |  `createTime` TIMESTAMP(3),
        |  `length` double,
        |  proctime as proctime()
        |) WITH (
        |  'datasource' = 'kafka_test',  -- 数据源别名定义在common.properties中，也可通过@Config注解定义
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin)
  }

  @Step3("kafka数据与mysql维表关联")
  def showJoin: Unit = {
    sql(
      """
        |select
        | t1.id, t2.name
        |from t_kafka_fire t1
        |   left join t_mysql_dim for system_time as of t1.proctime as t2 on t1.id=t2.id
        |group by t1.id, t2.name
        |""".stripMargin).print()
  }
}
