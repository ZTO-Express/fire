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

package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |hive.cluster=test
    |fire.rest.filter.enable=false
    |""")
object FlinkSQLParseTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val select = "select t1.id,t1.name from ods.test t1 where t1.a > 1"
    val selectJoin = "select t1.id,t2.name from tmp.test t1 left join ods.t_user t2 right join tmp.baseuser t3 on t1.id=t3.id where t1.a > 1"
    val insertInto = s"insert into dim.t_sink_table partition(ds='20210619') ${selectJoin}"
    val insertOverwrite = "insert overwrite dw.kwang_test partition(ds='202106', city='beijing') values(4,'zz')"
    val createView = s"create view t_view as ${selectJoin}"
    val createTableAsSelect = s"CREATE TABLE t_baseuser like tmp.test"
    val createKafkaTable =
      """
        |CREATE TABLE tmp.test (
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
        |""".stripMargin
    val alterTableAddPartitionStatement =
      """
        |alter table tmp.t_user add partition (ds='20210620', city = 'beijing')
        |""".stripMargin
    val dropTable =
      """
        |drop table if exists tmp.test
        |""".stripMargin
    val renameTable =
      """
        |alter table tmp.t_user rename to ods.t_user2
        |""".stripMargin
    val dropPartition =
      """
        |ALTER TABLE tmp.food DROP PARTITION (ds='20151219', city = 'beijing')
        |""".stripMargin

    /*val dropDB = "drop database tmp"
    this.fire.sql("create database tmp")
    this.fire.sql(createKafkaTable)
    this.fire.sql("select * from tmp.t_student").print()*/
    // this.fire.sql(alterTableAddPartitionStatement)
    this.fire.sql("create database tmp")
    this.fire.sql("create database dim")
    this.fire.sql("create database ods")
    this.fire.sql(createKafkaTable)
    this.fire.sql(selectJoin)
  }

}
