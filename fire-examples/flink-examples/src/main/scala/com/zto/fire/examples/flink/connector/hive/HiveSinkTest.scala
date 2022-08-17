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

package com.zto.fire.examples.flink.connector.hive

import com.zto.fire._
import com.zto.fire.flink.FlinkStreaming

/**
 * 基于fire框架进行Flink SQL开发<br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/4b9179fa090b9360.md'>1. Flink SQL开发官方文档——kafka connector</a><br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/a7dfbfd1c259be68.md'>2. Flink SQL开发官方文档——jdbc connector</a>
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-18 17:24
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
object HiveSinkTest extends FlinkStreaming {

  // 具体的业务逻辑放到process方法中
  override def process: Unit = {
    this.fire.disableOperatorChaining()
    sql(
      """
        |CREATE TABLE t_student (
        |  `table` STRING,
        |  `before` ROW(`id` bigint, `age` int, `name` string, `length` double, `createTime` string),	-- 嵌套json的声明方式，使用ROW()，这么写很麻烦，但没办法
        |  `after` ROW(id bigint, age int, name string, length double, createTime string)
        |) WITH (
        |  'connector' = 'kafka',								-- 用于指定connector的类型
        |  'topic' = 'fire',										-- 消费的topic名称为fire
        |  'properties.bootstrap.servers' = 'kafka-server:9092',	-- kafka的broker地址
        |  'properties.group.id' = 'fire',						-- 当前flink sql任务所使用的groupId
        |  'scan.startup.mode' = 'earliest-offset',				-- 指定从什么位置开始消费
        |  'format' = 'json'										-- 指定解析的kafka消息为json格式
        |)
        |""".stripMargin)
    sql(
      """
        |create view v_student as
        |select
        |	t.`table` as table_name,
        |	after.id as id, 									-- 解析ROW类型声明的嵌套字段，直接以点的方式一级一级指定
        |	after.age as age,
        |	after.name as name,
        |	after.length as length,
        |	after.createTime as create_time
        |from t_student t
        |""".stripMargin)
    this.fire.useHiveCatalog()
    println(this.tableEnv.getCurrentCatalog)
    sql(
      """
        |CREATE TABLE if not exists tmp.flink_hive_sink (
        |  id BIGINT,
        |  name STRING,
        |  age INT
        |) PARTITIONED BY (ds STRING) STORED AS textfile TBLPROPERTIES (
        |  'sink.partition-commit.trigger'='partition-time',               -- 分区触发提交
        |  'sink.partition-commit.delay'='0 s',      -- 提交延迟
        |  'sink.partition-commit.policy.kind'='metastore,success-file'    -- 提交类型
        |)
        |""".stripMargin)
    sql(
      """
        |INSERT INTO TABLE hive.tmp.flink_hive_sink SELECT id, name, age, DATE_FORMAT(create_time, 'yyyyMMdd') FROM default_catalog.default_database.v_student
        |""".stripMargin)
  }
}
