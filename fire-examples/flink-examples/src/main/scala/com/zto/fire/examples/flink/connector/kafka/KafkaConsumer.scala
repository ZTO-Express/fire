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

package com.zto.fire.examples.flink.connector.kafka

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.flink.FlinkStreaming

/**
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |flink.sql.conf.table.exec.state.ttl   =       1 ms
    |""")
object KafkaConsumer extends FlinkStreaming {

  override def process: Unit = {
    // this.insertPrint
    this.streamJoin
  }

  def streamJoin: Unit = {
    val table = this.flink.sql(
      """
        |CREATE TABLE kafka (
        |  id int,
        |  name string,
        |  age int,
        |  length string,
        |  before row<bill_code string, bage int>,
        |  code as before.bill_code,
        |  bage as before.bage,
        |  sex boolean
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'fire',
        |  'properties.bootstrap.servers' = 'kafka-server:9092',
        |  'properties.group.id' = 'fire',
        |  'scan.startup.mode' = 'latest-offset',
        |  'value.format' = 'json'
        |)
        |""".stripMargin)

    this.flink.sql(
      """
        |CREATE TABLE kafka2 (
        |  id int,
        |  name string,
        |  age int,
        |  length string,
        |  before row<bill_code string, bage int>,
        |  code as before.bill_code,
        |  bage as before.bage,
        |  sex boolean
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'fire2',
        |  'properties.bootstrap.servers' = 'kafka-server:9092',
        |  'properties.group.id' = 'fire2',
        |  'scan.startup.mode' = 'latest-offset',
        |  'value.format' = 'json'
        |)
        |""".stripMargin)

    sql(
      """
        |create view kafka_join
        |as
        |select
        |   k1.id,
        |   k2.name,
        |   k2.before.bill_code as bill_code,
        |   k1.bage,
        |   k2.bage
        |from kafka k1 left join kafka2 k2
        |   on k1.before.bill_code=k2.code
        |where k1.bage > 10
        |""".stripMargin)

    sql(
      """
        |select * from kafka_join
        |""".stripMargin).print()
  }

  def insertPrint: Unit = {
    this.flink.sql(
      """
        |CREATE TABLE kafka (
        |  id int,
        |  name string,
        |  age int,
        |  length string,
        |  before row<bill_code string, bage int>,
        |  -- code as before.bill_code,
        |  -- bage as before.bage,
        |  sex boolean
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'fire',
        |  'properties.bootstrap.servers' = 'kafka-server:9092',
        |  'properties.group.id' = 'fire',
        |  'scan.startup.mode' = 'latest-offset',
        |  'value.format' = 'json'
        |)
        |""".stripMargin)

    sql(
      """
        |create table `print` with('connector' = 'print') like kafka (EXCLUDING ALL)
        |""".stripMargin)

    sql(
      """
        |insert into print select * from kafka
        |""".stripMargin)
  }
}
