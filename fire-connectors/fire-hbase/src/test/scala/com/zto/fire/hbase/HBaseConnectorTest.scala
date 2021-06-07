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

package com.zto.fire.hbase

import com.zto.fire.common.anno.{Internal, TestStep}
import com.zto.fire.common.db.bean.Student
import com.zto.fire.common.util.{DatasourceManager, PropUtils}
import com.zto.fire.predef._
import org.junit.Assert._
import org.junit.{Before, Test}

/**
 * 用于单元测试HBaseConnector中的API
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-13 15:06
 */
class HBaseConnectorTest {
  val tableName = "fire_test_1"
  val tableName2 = "fire_test_2"
  var hbase: HBaseConnector = null
  var hbase2: HBaseConnector = null

  @Before
  def init: Unit = {
    PropUtils.load("HBaseConnectorTest")
    this.hbase = HBaseConnector()
    this.hbase2 = HBaseConnector()
    this.hbase2 = HBaseConnector(keyNum = 2)
  }

  /**
   * 用于测试以下api：
   * 1. 判断表是否存在
   * 2. disable 表
   * 3. create 表
   */
  @Test
  @TestStep(step = 1, desc = "创建表API测试")
  def testDDL: Unit = this.createTestTable

  /**
   * 测试表是否存在的缓存功能
   */
  @Test
  @TestStep(step = 2, desc = "增删改查API测试")
  def testTableExists: Unit = {
    val starTime = currentTime
    (1 to 10).foreach(i => {
      this.hbase.tableExists(this.tableName)
    })
    println("未开启缓存总耗时：" + (timecost(starTime)))

    val starTime2 = currentTime
    (1 to 10).foreach(i => {
      this.hbase.isExists(this.tableName)
    })
    println("开启缓存总耗时：" + (timecost(starTime2)))
  }

  /**
   * 测试插入多条记录
   */
  @Test
  @TestStep(step = 3, desc = "增删改查API测试")
  def testInsert: Unit = {
    this.hbase.truncateTable(this.tableName)
    // 批量插入
    val studentList = Student.build(5)
    this.hbase.insert(this.tableName, studentList: _*)

    // get操作
    println("===========get=============")
    val rowKeyList = (1 to 5).map(i => i.toString)
    val getStudentList = this.hbase.get(this.tableName, classOf[Student], rowKeyList: _*)
    assertEquals(getStudentList.size, 5)
    getStudentList.foreach(println)
    val getOne = this.hbase.get(this.tableName, classOf[Student], HBaseConnector.buildGet("1"))
    assertEquals(getOne.size, 1)

    println("===========scan=============")
    val scanList = this.hbase.scan(this.tableName, classOf[Student], "1", "3")
    assertEquals(scanList.size, 2)
    scanList.foreach(println)

    for (i <- 1 to 5) {
      DatasourceManager.get.foreach(t => {
        t._2.foreach(source => {
          println("数据源：" + t._1.toString + " " + source)
        })
      })
      println("=====================================")
      Thread.sleep(10000)
    }
  }

  /**
   * 测试跨集群支持
   */
  @Test
  @TestStep(step = 4, desc = "多集群测试")
  def testMultiCluster: Unit = {
    this.hbase.truncateTable(this.tableName)
    this.hbase2.truncateTable(this.tableName2)
    val studentList1 = Student.build(5)
    this.hbase.insert(this.tableName, studentList1: _*)
    val scanStudentList1 = this.hbase.scan(this.tableName, classOf[Student], "1", "6")
    assertEquals(scanStudentList1.size, 5)
    val studentList2 = Student.build(3)
    this.hbase2.insert(this.tableName2, studentList2: _*)
    val scanStudentList2 = this.hbase2.scan(this.tableName2, classOf[Student], "1", "6")
    assertEquals(scanStudentList2.size, 3)

    assertEquals(DatasourceManager.get.size(), 1)
    DatasourceManager.get.foreach(t => {
      t._2.foreach(println)
    })
  }

  /**
   * 测试多版本插入
   * 注：多版本需要在Student类上声明@HConfig注解：@HConfig(nullable = true, multiVersion = true)
   */
  @Test
  @TestStep(step = 5, desc = "多版本测试")
  def testMultiInsert: Unit = {
    this.hbase2.truncateTable(this.tableName2)
    val studentList = Student.build(5)
    this.hbase2.insert(this.tableName2, studentList: _*)
    val students = this.hbase2.get(this.tableName2, classOf[Student], "1", "2")
    students.foreach(println)
  }

  /**
   * 测试老的api使用方式
   */
  @Test
  @TestStep(step = 6, desc = "静态类型API测试")
  def testOldStyle: Unit = {
    val hbaseConn1 = HBaseConnector(keyNum = 2)
    val hbaseConn2 = HBaseConnector(keyNum = 2)
    assertEquals(hbaseConn1 == hbaseConn2, true)
    println(HBaseConnector.tableExists("fire_test_1"))
    println(HBaseConnector.tableExists("fire_test_1"))
  }

  /**
   * 创建必要的表信息
   */
  @Internal
  private def createTestTable: Unit = {
    if (this.hbase.isExists(this.tableName)) this.hbase.dropTable(this.tableName)
    assertEquals(this.hbase.isExists(this.tableName), false)
    this.hbase.createTable(this.tableName, "info", "data")
    assertEquals(this.hbase.isExists(this.tableName), true)

    if (this.hbase2.isExists(this.tableName2)) this.hbase2.dropTable(this.tableName2)
    assertEquals(this.hbase2.isExists(this.tableName2), false)
    this.hbase2.createTable(this.tableName2, "info", "data")
    assertEquals(this.hbase2.isExists(this.tableName2), true)
  }

}
