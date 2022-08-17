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

package com.zto.fire.examples.flink.connector

import com.zto.fire.common.conf.FireKafkaConf
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Checkpoint

@Checkpoint(60)
object FlinkHudiTest extends FlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {

    var sql =
      """
        |CREATE TABLE hudi_table_test(
        |  uuid VARCHAR(20),
        |  action VARCHAR(10),
        |  age INT,
        |  ts BIGINT,
        |  ds VARCHAR(20)
        |)
        |PARTITIONED BY (ds)
        |WITH (
        |  'connector' = 'hudi',
        |  'path' = 'hdfs:///user/flink/huditest/hudi_table_test',
        |  'table.type' = 'MERGE_ON_READ',
        |  'compaction.delta_commits' = '3',
        |  'compaction.delta_seconds' = '300',
        |  'hoodie.datasource.write.hive_style_partitioning' = 'true'
        |)
        |""".stripMargin

    this.tableEnv.executeSql(sql)

    sql =
      s"""
        |CREATE TABLE kafka_source_table (
        |  uuid VARCHAR(20),
        |  action VARCHAR(10),
        |  age INT,
        |  ts BIGINT,
        |  ds VARCHAR(20)
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'kafka_hudi_test',
        |  'properties.bootstrap.servers' = '${FireKafkaConf.kafkaBrokers()}',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin

    this.tableEnv.executeSql(sql)

    sql =
      """
        |INSERT INTO hudi_table_test SELECT uuid,action,age,ts,ds FROM kafka_source_table
        |""".stripMargin

    this.tableEnv.executeSql(sql)

  }
}