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
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

@Config(
  """
    |flink.fire.rest.filter.enable       =       false
    |flink.default.parallelism           =       8
    |flink.max.parallelism               =       8
    |""")
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object FlinkRetractStreamTest extends FlinkStreaming {

  val tableName = "spark_test"

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().map(json => JSONUtils.parseObject[Student](json)).shuffle
    dstream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select name, age, createTime, length, sex from student group by name, age, createTime, length, sex")

    val fields = "name, age, createTime, length, sex"
    val sql = s"INSERT INTO $tableName ($fields) VALUES (?, ?, ?, ?, ?)"
    // 方式一、table中的列顺序和类型需与jdbc sql中的占位符顺序保持一致
    table.jdbcBatchUpdate(sql, keyNum = 10)
    // 方式二、自定义row取数规则，该种方式较灵活，可定义取不同的列，顺序仍需与sql占位符保持一致
    table.jdbcBatchUpdate2(sql, batch = 10, flushInterval = 10000, keyNum = 10)(row => Seq(row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4)))

    // toRetractStream支持状态更新、删除操作，比例sql中含有group by 等聚合操作，后进来的记录会导致已有的聚合结果不正确
    // 使用toRetractStream后会将之前的旧的聚合结果重新发送一次，并且tuple中的flag标记为false，然后再发送一条正确的结果
    // 类似于structured streaming中自动维护结果表，并进行update操作
    this.tableEnv.asInstanceOf[StreamTableEnvironment].toRetractStream[Row](table).print()
  }
}
