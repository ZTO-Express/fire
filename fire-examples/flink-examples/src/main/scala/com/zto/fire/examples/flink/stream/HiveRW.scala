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
import com.zto.fire.core.anno.{Hive, Kafka}
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * 基于Fire进行Flink Streaming开发
 */
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HiveRW extends BaseFlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  override def process: Unit = {
    this.fire.useHiveCatalog()

    this.ddl

    this.fire.sql(
      """
        |insert into table tmp.baseorganize_fire select * from dim.baseorganize limit 10
        |""".stripMargin)

    this.fire.sql(
      """
        |select * from tmp.baseorganize_fire
        |""".stripMargin).print()
  }

  /**
   * 创建表
   */
  def ddl: Unit = {
    this.fire.sql(
      """
        |drop table if exists tmp.baseorganize_fire
        |""".stripMargin)

    this.fire.sql(
      """
        |create table tmp.baseorganize_fire (
        |    id bigint,
        |    name string,
        |    age int
        |) partitioned by (ds string)
        |row format delimited fields terminated by '/t'
        |""".stripMargin)
  }
}