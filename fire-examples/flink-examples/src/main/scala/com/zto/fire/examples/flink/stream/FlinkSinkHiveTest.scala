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

package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * 基于fire框架进行Flink SQL开发<br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/4b9179fa090b9360.md'>1. Flink SQL开发官方文档——kafka connector</a><br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/a7dfbfd1c259be68.md'>2. Flink SQL开发官方文档——jdbc connector</a>
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-18 17:24
 */
object FlinkSinkHiveTest extends BaseFlinkStreaming {

  // 具体的业务逻辑放到process方法中
  override def process: Unit = {
    // 消息
    """
      |{"table":"t_student", "order_time": "2021-07-26 15:58:59.181","before":{"id":1,"age":1,"name":"spark1","length":51.1,"createTime":"2021-06-20 11:31:51"},"after":{"id":1,"age":21,"name":"flink1","length":151.1,"createTime":"2021-06-22 10:31:30"}}
      |{"table":"t_student", "order_time": "2021-07-27 15:58:59.181","before":{"id":2,"age":2,"name":"spark2","length":52.2,"createTime":"2021-06-20 11:32:52"},"after":{"id":2,"age":22,"name":"flink2","length":152.2,"createTime":"2021-06-23 10:32:30"}}
      |{"table":"t_student", "order_time": "2021-07-31 15:58:59.181","before":{"id":3,"age":3,"name":"spark3","length":53.3,"createTime":"2021-06-20 11:33:53"},"after":{"id":3,"age":23,"name":"flink3","length":153.3,"createTime":"2021-06-24 10:33:30"}}
      |{"table":"t_student", "order_time": "2021-07-29 15:58:59.181","before":{"id":4,"age":4,"name":"spark4","length":54.4,"createTime":"2021-07-30 11:34:54"},"after":{"id":4,"age":24,"name":"flink4","length":154.4,"createTime":"2021-07-30 10:34:30"}}
      |{"table":"t_student", "order_time": "2021-07-30 15:58:59.181","before":{"id":5,"age":5,"name":"spark5","length":55.5,"createTime":"2021-07-29 11:35:55"},"after":{"id":5,"age":25,"name":"flink5","length":155.5,"createTime":"2021-07-29 09:35:30"}}
      |""".stripMargin

    this.fire.sql(
      """
        |CREATE TABLE t_student (
        |  `table` STRING,
        |  `before` ROW(`id` bigint, `age` int, `name` string, `length` double, `createTime` string),	-- 嵌套json的声明方式，使用ROW()，这么写很麻烦，但没办法
        |  `after` ROW(id bigint, age int, name string, length double, createTime string),
        |  order_time TIMESTAMP(3),
        |  WATERMARK FOR order_time AS order_time - INTERVAL '50' SECOND
        |) WITH (
        |  'connector' = 'kafka',								-- 用于指定connector的类型
        |  'topic' = 'fire',										-- 消费的topic名称为fire
        |  'scan.startup.mode'='latest-offset',
        |  'properties.bootstrap.servers' = 'kafka-server:9092',	-- kafka的broker地址
        |  'properties.group.id' = 'fire2',						-- 当前flink sql任务所使用的groupId
        |  'format' = 'json'										-- 指定解析的kafka消息为json格式
        |)
        |""".stripMargin)
    this.fire.sql(
      """
        |create view v_student as
        |select
        |	t.`table` as table_name,
        |	after.id as id, 									-- 解析ROW类型声明的嵌套字段，直接以点的方式一级一级指定
        |	after.age as age,
        |	after.name as name,
        |	after.length as length,
        |	order_time as order_time
        |from t_student t
        |""".stripMargin)
    this.tableEnv.useHiveCatalog()
    println(this.tableEnv.getCurrentCatalog)
    this.fire.sql("drop table if exists tmp.flink_hive_sink")
    this.fire.sql(
      """
        |CREATE TABLE if not exists tmp.flink_hive_sink (
        |  id BIGINT,
        |  name STRING,
        |  age INT
        |) PARTITIONED BY (ds STRING)
        |STORED AS parquet
        |TBLPROPERTIES (
        |  'partition.time-extractor.kind'='custom',
        |  'partition.time-extractor.timestamp-pattern'='$ds',    -- 与分区字段对应
        |  'sink.partition-commit.trigger'='partition-time',               -- 分区触发提交
        |  'partition.time-extractor.class'='com.zto.fire.flink.util.HivePartitionTimeExtractor',
        |  'sink.partition-commit.delay'='1 s',      -- 提交延迟
        |  'sink.partition-commit.policy.kind'='metastore,success-file'    -- 提交类型
        |)
        |""".stripMargin)
    this.fire.sql(
      """
        |INSERT INTO TABLE hive.tmp.flink_hive_sink SELECT id, name, age, DATE_FORMAT(order_time, 'yyyyMMdd') as ds FROM default_catalog.default_database.v_student
        |""".stripMargin)
  }
}
