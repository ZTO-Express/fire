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

package com.zto.fire.examples.spark.lineage

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.{HBase, Jdbc, Kafka, RocketMQ}
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.sync.SparkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@HBase("test")
@Config(
  """
    |fire.lineage.run.initialDelay=10
    |fire.shutdown.auto.exit=false
    |""")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@RocketMQ(brokers = "bigdata_test", topics = "fire2", groupId = "fire")
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
object SparkCoreLineageTest extends SparkCore {
  private val hbaseTable = "fire_test_1"
  private lazy val tableName = "spark_test"

  override def process: Unit = {
    (1 to 10).foreach(x => {
      val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
      df.rdd.foreachPartition(it => {
        val timestamp = DateFormatUtils.formatCurrentDateTime()
        val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
        this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
        HBaseConnector.get[Student](hbaseTable, classOf[Student], Seq("1"))
      })
      // 每个批次插100条
      df.hbasePutDF(this.hbaseTable, classOf[Student])
      Thread.sleep(10000)
    })

    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    df.rdd.foreachPartition(it => {
      val a = 1 / 0
    })
  }
}
