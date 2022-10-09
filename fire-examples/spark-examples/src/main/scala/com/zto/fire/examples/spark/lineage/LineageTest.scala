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
import com.zto.fire.core.anno.connector.{HBase, Hive, Jdbc, Kafka, RocketMQ}
import com.zto.fire.core.anno.lifecycle.{Process, Step1}
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.hive.HiveMetadataTest.{multiPartitionTable, sql}
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming
import com.zto.fire.spark.sync.SparkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@HBase("test")
@Hive("test")
@Config("""fire.lineage.run.initialDelay=10""")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Streaming(interval = 10, concurrent = 2, backpressure = true, maxRatePerPartition = 100)
@RocketMQ(brokers = "bigdata_test", topics = "fire2", groupId = "fire")
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
object LineageTest extends SparkStreaming {
  private val hbaseTable = "fire_test_1"
  private lazy val tableName = "spark_test"

  @Process
  def source: Unit = {
    this.fire.createKafkaDirectStream().print()
    sql(
      s"""
         |insert into table ${multiPartitionTable} partition(ds, city) select *,'sh' as city from dw.mdb_md_dbs where ds='20211001' limit 100
         |""".stripMargin)
    val dstream = this.fire.createRocketMqPullStream().map(t => JSONUtils.toJSONString(t))
    dstream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val timestamp = DateFormatUtils.formatCurrentDateTime()
        val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
        this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
        HBaseConnector.get[Student](hbaseTable, classOf[Student], Seq("1"))
      })

      val studentList = Student.newStudentList()
      val studentDF = this.fire.createDataFrame(studentList, classOf[Student])
      // 每个批次插100条
      studentDF.hbasePutDF(this.hbaseTable, classOf[Student])
    })
    dstream.print()
  }

  @Step1("周期性执行")
  def test: Unit = {
    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(SparkLineageAccumulatorManager.getValue))
    }, 0, 60, TimeUnit.SECONDS)
  }
}
