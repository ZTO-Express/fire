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

package com.zto.fire.examples.spark.structured

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector.{HBase, Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseStructuredStreaming

/**
 * 结构化流测试
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Hive("test")
@HBase("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object JdbcSinkTest extends BaseStructuredStreaming {

  override def process: Unit = {
    // 接入kafka并解析json，支持大小写，默认表名为kafka
    val kafkaDataset = this.fire.loadKafkaParseJson()
    // 直接使用或sql
    /*kafkaDataset.print()
    sql("select * from kafka").print()*/

    // jdbc的sql语句
    val insertSql = "insert into spark_test(name, age, createTime, length, sex, rowKey) values(?,?,?,?,?,?)"

    // 将流数据持续写入到关系型数据库中（插入部分列）
    kafkaDataset.select("data.name", "data.age", "data.createTime", "data.length", "data.sex", "data.rowKey").jdbcBatchUpdate(insertSql, keyNum = 6)
    // 插入所有列并在Seq中列举DataFrame指定顺序，该顺序必须与insertSql中的问号占位符存在绑定关系
    kafkaDataset.select("data.*").jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex", "rowKey"), keyNum = 6)

    this.fire.createDataFrame(Student.newStudentList(), classOf[Student]).createOrReplaceTempViewCache("student")
    sql(
      """
        |select
        | t.name,
        | s.length
        |from kafka t left join student s
        | on t.name=s.name
        |""".stripMargin).print()
  }
}
