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
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.{Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import org.apache.flink.api.scala._

/**
 * Flink 整合hive维表的例子，在流中join hive 维表数据
 * 注：流关联hive维表需要以下6个步骤：
 *  1. 开启hint：sql.conf.table.dynamic-table-options.enabled=true
 *  2. 切换为hive catalog以及hive方言：this.fire.useHiveCatalog()
 *  3. SQL查询维表信息并附有hint： select * from hiveTable /*+ OPTIONS('streaming-source.enable' = 'true','streaming-source.monitor-interval' = '15 s','streaming-source.partition-order'='create-time')*/
 *  4. 切回默认catalog以及方言：this.fire.useDefaultCatalog
 *  5. 将hive维表数据注册为临时表：dimTable.createOrReplaceTempView("baseorganize")
 *  6. SQL中流关联hive临时表：join xxx on
 *
 * @author ChengLong 2020年4月3日 09:05:53
 */
@Config(
  """
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire
    |# 1. 读取hive维表必须启用该配置
    |sql.conf.table.dynamic-table-options.enabled=true
    |""")
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
@Checkpoint(interval = 60, concurrent = 1, pauseBetween = 60, timeout = 60)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object FlinkHiveTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().filter(json => JSONUtils.isJson(json))
      .map(json => JSONUtils.parseObject[Student](json))
      .setParallelism(2)
    dstream.createOrReplaceTempView("student")

    // 2. 切换hive catalog以及方言，表示从hive中读取维表数据
    this.fire.useHiveCatalog()
    val dimTable = this.fire.sqlQuery(
      """
        |select id,shortname
        |from tmp.baseorganize_flink
        |-- 3. 指定以下hit，用于指明flink定时ttl掉维表数据
        |/*+ OPTIONS('streaming-source.enable' = 'true', -- 开启流式读取 Hive 数据
        |'streaming-source.partition.include' = 'all', -- 1.latest 属性: 只读取最新分区数据。2.all: 读取全量分区数据 ，默认值为 all，表示读所有分区，latest 只能用在 temporal join 中，用于读取最新分区作为维表，不能直接读取最新分区数据
        |'streaming-source.monitor-interval' = '1 h',  -- 指定ttl的间隔时间，监听新分区生成的时间、不宜过短 、最短是1 个小时，因为目前的实现是每个 task 都会查询 metastore，高频的查可能会对metastore 产生过大的压力。需要注意的是，1.12.1 放开了这个限制，但仍建议按照实际业务不要配个太短的 interval
        |'streaming-source.partition-order'='create-time')*/ -- 非hive分区表，需要指定create-time，如果是分区表：partition-name 使用默认分区名称顺序加载最新分区2.create-time 使用分区文件创建时间顺序, 3. partition-time 使用分区时间顺序
        |""".stripMargin)

    // 4. 切换回默认的catalog后再将hive维表数据注册为临时表，避免在default catalog查询不到baseorganize这张临时表
    this.fire.useDefaultCatalog
    // 5. 将hive维表数据注册为临时表
    // dimTable.createOrReplaceTempView("baseorganize")

    // 5. 关联流表与hive维表，当hive维表更新后flink会自动周期性的刷新维表数据，并体现在关联的结果中
    this.fire.sql(
      s"""
        |select s.id,s.name,b.shortname
        |from student s
        |left join $dimTable b
        |on s.id=b.id
        |""".stripMargin).print()

    this.fire.start
  }
}
