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
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.core.anno.lifecycle._
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * 用于演示通过flink sql写hive表
 *
 * @author ChengLong 2022-08-23 13:03:20
 * @since 2.0.0
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Hive("test")
@Streaming(interval = 60, parallelism = 2)
object HiveWriteDemo extends FlinkStreaming {

  @Step1("创建hive表数据源")
  def hiveTable: Unit = {
    // 定义hive表前先切换到hive catalog
    this.fire.useHiveCatalog()
    sql(
      """
        |CREATE TABLE if not exists `t_hive_table` (
        |  `id` BIGINT,
        |  `name` STRING,
        |  `age` INT,
        |  `createTime` TIMESTAMP,
        |  `length` double
        |) PARTITIONED BY (ds STRING) STORED AS orc TBLPROPERTIES (
        | 'partition.time-extractor.timestamp-pattern'='$ds',
        | 'sink.partition-commit.trigger'='process-time',
        | 'sink.partition-commit.delay'='1 min',
        | 'sink.partition-commit.policy.kind'='metastore,success-file'
        |)
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
        |  `length` double
        |) WITH (
        |  'datasource' = 'kafka_test',  -- 数据源别名定义在common.properties中，也可通过@Config注解定义
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json'
        |);
        |""".stripMargin)
  }

  @Step3("将kafka数据写入到hive表中")
  def sinkToHive: Unit = {
    sql(
      """
        |insert into hive.tmp.t_hive_table
        |select
        |  `id`,
        |  `name`,
        |  `age`,
        |  `createTime`,
        |  `length`,
        |  DATE_FORMAT(LOCALTIMESTAMP,'yyyyMMdd') as ds
        |from `t_kafka_fire` d
        |""".stripMargin)
  }
}
