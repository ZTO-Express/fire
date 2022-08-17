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
import com.zto.fire.core.anno.connector.Hive
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
@Hive("test")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HiveBatchSinkTest extends FlinkStreaming {

  // 具体的业务逻辑放到process方法中
  override def process: Unit = {
    this.fire.useHiveCatalog()
    sql("drop table if exists tmp.flink_hive_sink4")
    sql(
      """
        |CREATE TABLE if not exists tmp.flink_hive_sink4 (
        |   bill_num BIGINT,
        |   disorsen_man_code STRING
        | ) PARTITIONED BY (ds STRING) STORED AS textfile
        |""".stripMargin)
    sql(
      """
        |insert overwrite table tmp.flink_hive_sink4 select bill_num,disorsen_man_code,ds from dw.zto_rn_bill_statis limit 10
        |""".stripMargin)
  }
}
