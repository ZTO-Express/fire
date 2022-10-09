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

import com.zto.fire.flink.FlinkStreaming
import com.zto.fire._
import com.zto.fire.flink.anno.Streaming

/**
 * 基于Fire框架开发Flink SQL的示例代码
 *
 * @author ChengLong 2022-08-23 16:17:36
 * @since 2.3.1
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(interval = 60, parallelism = 2)
object SimpleSqlDemo extends FlinkStreaming {

  override def process: Unit = {
    sql(
      """
        |CREATE TABLE t_student (
        |  `table` STRING,
        |  `before` ROW(`id` bigint, `age` int, `name` string, `length` double, `createTime` string),	-- 嵌套json的声明方式，使用ROW()，这么写很麻烦，但没办法
        |  `after` ROW(id bigint, age int, name string, length double, createTime string),
        |  order_time TIMESTAMP(3),
        |  WATERMARK FOR order_time AS order_time - INTERVAL '50' SECOND
        |) WITH (
        |  'connector' = 'kafka',								-- 用于指定connector的类型
        |  'topic' = 'fire-sql',										-- 消费的topic名称为fire
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
        |	order_time as create_time
        |from t_student t
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE sink (
        |  id BIGINT,
        |  name STRING,
        |  age INT,
        |  `count` bigint,
        |  PRIMARY KEY (id) NOT ENFORCED							-- 指定主键字段，如果insert语句后面的select代码聚合算子（group by），则必须指定主键，用于数据的更新操作
        |) WITH (
        |   'connector' = 'jdbc',								-- 指定当前connector为jdbc类型
        |   'url' = 'jdbc:mysql://mysql-server:3306/fire',		-- jdbc的url
        |   'table-name' = 'flink_sql_test',						-- 指定往哪张数据库表中写数据，表示往mysql的名为flink_sql_test的表插入或更新数据
        |   'driver' = 'com.mysql.jdbc.Driver',					-- jdbc的驱动类名
        |   'username' = 'root',									-- jdbc的用户名
        |   'password' = 'fire',							-- jdbc的密码
        |   'sink.buffer-flush.interval' = '10s',			    -- 标识每隔10s钟将数据flush一次到mysql中，避免逐条insert效率低
        |   'sink.buffer-flush.max-rows' = '3',					-- 标识积累满3条执行一次批量insert，通用避免逐条insert，和sink.buffer-flush.interval先符合为准
        |   'sink.max-retries' = '3'								-- 插入失败时重试几次
        |)
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE sink2 (
        |  id BIGINT,
        |  name STRING,
        |  age INT,
        |  `count` bigint,
        |  PRIMARY KEY (id) NOT ENFORCED							-- 指定主键字段，如果insert语句后面的select代码聚合算子（group by），则必须指定主键，用于数据的更新操作
        |) WITH (
        |   'connector' = 'jdbc',								-- 指定当前connector为jdbc类型
        |   'url' = 'jdbc:mysql://mysql-server:3306/fire',		-- jdbc的url
        |   'table-name' = 'flink_sql_test2',						-- 指定往哪张数据库表中写数据，表示往mysql的名为flink_sql_test的表插入或更新数据
        |   'driver' = 'com.mysql.jdbc.Driver',					-- jdbc的驱动类名
        |   'username' = 'root',									-- jdbc的用户名
        |   'password' = 'fire',							-- jdbc的密码
        |   'sink.buffer-flush.interval' = '10s',			    -- 标识每隔10s钟将数据flush一次到mysql中，避免逐条insert效率低
        |   'sink.buffer-flush.max-rows' = '3',					-- 标识积累满3条执行一次批量insert，通用避免逐条insert，和sink.buffer-flush.interval先符合为准
        |   'sink.max-retries' = '3'								-- 插入失败时重试几次
        |)
        |""".stripMargin)

    sql("""
        |insert into sink
        |select id, name, age, sum(1) as `count`
        |from v_student
        |group by id,name,age;
        |
        |insert into sink2
        |select id, name, age, sum(1) as `count`
        |from v_student
        |group by id,name,age
        |""".stripMargin)
  }
}
