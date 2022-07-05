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

package com.zto.fire.examples.spark.hbase

import com.zto.fire.common.anno.TestStep
import com.zto.fire.common.util.DatasourceManager
import com.zto.fire.core.anno.{HBase, HBase2}
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.predef._
import com.zto.fire.spark.BaseSparkCore
import org.junit.Assert._
import org.junit.Test

/**
 * 用于单元测试HBaseConnector中的API
 *
 * @author ChengLong
 * @since 2.2.2
 * @date 2022-05-11 13:51:22
 */
@HBase("test")
@HBase2(cluster = "test", scanPartitions = 3, storageLevel = "DISK_ONLY")
class HBaseApiTest extends BaseSparkCore with HBaseBaseTester {

  /**
   * 用于测试以下api：
   * 1. 判断表是否存在
   * 2. disable 表
   * 3. create 表
   */
  @Test
  @TestStep(step = 0, desc = "DDL测试")
  def createTestTable: Unit = {
    if (HBaseConnector.isExists(this.tableName1)) HBaseConnector.dropTable(this.tableName1)
    assertEquals(HBaseConnector.isExists(this.tableName1), false)
    HBaseConnector.createTable(this.tableName1, Seq("info"))
    assertEquals(HBaseConnector.isExists(this.tableName1), true)

    if (HBaseConnector(2).isExists(this.tableName2)) HBaseConnector(2).dropTable(this.tableName2)
    assertEquals(HBaseConnector(2).isExists(this.tableName2), false)
    HBaseConnector(2).createTable(this.tableName2, "info", "data")
    assertEquals(HBaseConnector(2).isExists(this.tableName2), true)
  }

  /**
   * 测试表是否存在的缓存功能
   */
  @Test
  @TestStep(step = 2, desc = "增删改查API测试")
  def testTableExists: Unit = {
    val starTime = currentTime
    (1 to 10).foreach(i => {
      HBaseConnector.tableExists(this.tableName1)
    })
    println("未开启缓存总耗时：" + (elapsed(starTime)))

    val starTime2 = currentTime
    (1 to 10).foreach(i => {
      HBaseConnector.isExists(this.tableName1)
    })
    println("开启缓存总耗时：" + (elapsed(starTime2)))
  }

  /**
   * 测试插入多条记录
   */
  @Test
  @TestStep(step = 3, desc = "增删改查API测试")
  def testInsert: Unit = {
    // 批量插入
    val studentList = Student.newStudentList().toSeq
    HBaseConnector.insert(this.tableName1, studentList)

    // get操作
    val rowKeyList = (1 to 5).map(i => i.toString)
    val getStudentList = HBaseConnector.get(this.tableName1, classOf[Student], rowKeyList)
    assertEquals(getStudentList.size, 5)
    getStudentList.foreach(println)

    val scanList = HBaseConnector.scan(this.tableName1, classOf[Student], "2", "4")
    assertEquals(scanList.size, 2)
    scanList.foreach(println)
  }

  /**
   * 测试跨集群支持
   */
  @Test
  @TestStep(step = 4, desc = "多集群测试")
  def testMultiCluster: Unit = {
    HBaseConnector.truncateTable(this.tableName1)
    HBaseConnector(2).truncateTable(this.tableName2)
    val studentList1 = Student.newStudentList().toSeq
    HBaseConnector.insert(this.tableName1, studentList1)
    val scanStudentList1 = HBaseConnector.scan(this.tableName1, classOf[Student], "1", "6")
    assertEquals(scanStudentList1.size, 5)
    val studentList2 =Student.newStudentList()
    HBaseConnector(2).insert(this.tableName2, studentList2: _*)
    val scanStudentList2 = HBaseConnector(2).scan(this.tableName2, classOf[Student], "1", "6")
    assertEquals(scanStudentList2.size, 5)

    assertEquals(DatasourceManager.get.size(), 1)
  }

  /**
   * 测试多版本插入
   * 注：多版本需要在Student类上声明@HConfig注解：@HConfig(nullable = true, multiVersion = true)
   */
  @Test
  @TestStep(step = 5, desc = "多版本测试")
  def testMultiInsert: Unit = {
    val studentList = Student.newStudentList()
    HBaseConnector(2).insert(this.tableName2, studentList: _*)
    val students = HBaseConnector(2).get(this.tableName2, classOf[Student], "1", "2")
    assertEquals(students.size, 2)
  }
}
