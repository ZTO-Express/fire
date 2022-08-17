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
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{HBase, HBase2, HBase3, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import com.zto.fire.hbase.HBaseConnector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable.ListBuffer

/**
 * flink hbase sink
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-5-25 16:32:50
 */
@Checkpoint(30)
@HBase("test")
@HBase2("test") // 对应keyNum=2的Hbase集群地址
@HBase3("test") // 对应keyNum=3的Hbase集群地址
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HBaseTest extends FlinkStreaming {
  lazy val tableName = "fire_test_1"
  lazy val tableName2 = "fire_test_2"
  lazy val tableName3 = "fire_test_3"
  lazy val tableName5 = "fire_test_5"
  lazy val tableName6 = "fire_test_6"
  lazy val tableName7 = "fire_test_7"
  lazy val tableName8 = "fire_test_8"
  lazy val tableName9 = "fire_test_9"
  lazy val tableName10 = "fire_test_10"
  lazy val tableName11 = "fire_test_11"
  lazy val tableName12 = "fire_test_12"

  /**
   * table的hbase sink
   */
  def testTableHBaseSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sqlQuery("select id, name, age from student group by id, name, age")
    // 方式一、自动将row转为对应的JavaBean
    // 注意：table对象上调用hbase api，需要指定泛型
    table.hbasePutTable[Student](this.tableName).setParallelism(1)
    this.fire.hbasePutTable[Student](table, this.tableName2, keyNum = 2)

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2[Student](this.tableName3)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.fire.hbasePutTable2[Student](table, this.tableName5, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * table的hbase sink
   */
  def testTableHBaseSink2(stream: DataStream[Student]): Unit = {
    val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2(this.tableName6)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.flink.hbasePutTable2(table, this.tableName7, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink(stream: DataStream[Student]): Unit = {
    // 方式一、DataStream中的数据类型为HBaseBaseBean的子类
    // stream.hbasePutDS(this.tableName)
    this.fire.hbasePutDS[Student](stream, this.tableName8)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName9, keyNum = 2)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName10)(value => value)
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink2(stream: DataStream[Student]): Unit = {
    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName11)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName12, keyNum = 2)(value => value)
  }

  /**
   * hbase的基本操作
   */
  def testHBase: Unit = {
    // get操作
    val getList = ListBuffer(HBaseConnector.buildGet("1"))
    val student = HBaseConnector.get(this.tableName, classOf[Student], getList, 1)
    if (student != null) println(JSONUtils.toJSONString(student))
    // scan操作
    val studentList = HBaseConnector.scan(this.tableName, classOf[Student], HBaseConnector.buildScan("0", "9"), 1)
    if (studentList != null) println(JSONUtils.toJSONString(studentList))
    // delete操作
    HBaseConnector.deleteRows(this.tableName, Seq("1"))
  }


  override def process: Unit = {
    val stream = this.fire.createKafkaDirectStream().filter(t => JSONUtils.isLegal(t)).map(json => JSONUtils.parseObject[Student](json)).setParallelism(1)
    HBaseConnector.truncateTable(this.tableName)
    HBaseConnector.truncateTable(this.tableName2)
    HBaseConnector.truncateTable(this.tableName3)
    HBaseConnector.truncateTable(this.tableName5)
    this.testTableHBaseSink(stream)
    this.testStreamHBaseSink(stream)
    this.testStreamHBaseSink2(stream)
    this.testTableHBaseSink2(stream)
    this.testHBase
  }
}
