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

import com.zto.fire._
import com.zto.fire.common.anno.TestStep
import com.zto.fire.core.anno.connector.{HBase, HBase2}
import com.zto.fire.examples.bean.{Student, StudentMulti}
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkCore
import org.apache.hadoop.hbase.client.Get
import org.apache.spark.sql.Encoders
import org.junit.Test

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

/**
  * 在spark中使用java 同步 api (HBaseConnector) 的方式读写hbase表
  * 注：适用于少量数据的实时读写，更轻量
  *
  * @author ChengLong 2019-5-9 09:37:25
  */
@HBase("test")
@HBase2(cluster = "test", scanPartitions = 3, storageLevel = "DISK_ONLY")
class HBaseConnectorUnitTest extends SparkCore with HBaseTester {

  /**
    * 使用HBaseConnector插入一个集合，可以是list、set等集合
    * 但集合的类型必须为HBaseBaseBean的子类
    */
  @Test
  @TestStep(step = 1, desc = "testHbasePutList")
  def testHbasePutList: Unit = {
    this.putData

    val getList = ListBuffer[Get]()
    val rowKeys = Seq("1", "2", "3", "5", "6")
    rowKeys.map(rowkey => (getList += new Get(rowkey.getBytes(StandardCharsets.UTF_8))))
    // 获取多版本形式存放的记录，并获取最新的两个版本就
    val resultList = this.fire.hbaseGetList(this.tableName1, classOf[Student], getList)
    assert(resultList.size == 5)

    val resultList2 = this.fire.hbaseGetList2(this.tableName1, classOf[Student], rowKeys)
    assert(resultList2.size == 5)

    val scanResultList = this.fire.hbaseScanList2(this.tableName1, classOf[Student], "2", "6")
    assert(scanResultList.size == 4)
  }

  /**
    * 使用HBaseConnector插入一个rdd的数据
    * rdd的类型必须为HBaseBaseBean的子类
    */
  @Test
  @TestStep(step = 2, desc = "testHbasePutRDD")
  def testHbasePutRDD: Unit = {
    val studentList = Student.newStudentList()
    val studentRDD = this.fire.createRDD(studentList, 2)
    // 为空的字段不插入
    studentRDD.hbasePutRDD(this.tableName1)

    val getList = Seq("1", "2", "3", "5", "6")
    val getRDD = this.fire.createRDD(getList, 2)
    val resultRDD = this.fire.hbaseGetRDD(this.tableName1, classOf[Student], getRDD)
    assert(resultRDD.count() == 5)

    val scanResultRdd = this.fire.hbaseScanRDD2(this.tableName1, classOf[Student], "2", "6")
    assert(scanResultRdd.count() == 4)
  }

  /**
    * 使用HBaseConnector插入一个DataFrame的数据
    */
  @Test
  @TestStep(step = 3, desc = "testHBasePutDF")
  def testHBasePutDF: Unit = {
    val studentList = Student.newStudentList()
    val studentDF = this.fire.createDataFrame(studentList, classOf[Student])
    studentDF.hbasePutDF(this.tableName1, classOf[Student])

    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.fire.createRDD(getList, 3)
    // get到的结果以dataframe形式返回
    val resultDF = this.fire.hbaseGetDF(this.tableName1, classOf[Student], getRDD)
    assert(resultDF.count() == 6)

    val dataFrame = this.fire.hbaseScanDF2(this.tableName1, classOf[Student], "2", "6")
    assert(dataFrame.count() == 4)
  }

  /**
    * 使用HBaseConnector插入一个Dataset的数据
    * dataset的类型必须为HBaseBaseBean的子类
    */
  @Test
  @TestStep(step = 4, desc = "testHBasePutDS")
  def testHBasePutDS: Unit = {
    val studentList = Student.newStudentList()
    val studentDS = this.fire.createDataset(studentList)(Encoders.bean(classOf[Student]))
    // 以多版本形式插入
    studentDS.hbasePutDS(this.tableName1, classOf[Student])

    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.fire.createRDD(getList, 2)
    // 指定在多版本获取时只取最新的两个版本
    val resultDS = this.fire.hbaseGetDS(this.tableName1, classOf[Student], getRDD)
    println(resultDS.count())
    assert(resultDS.count() == 6)

    val dataSet = this.fire.hbaseScanDS2(this.tableName1, classOf[Student], "2", "6")
    assert(dataSet.count() == 4)
  }

  /**
    * 根据指定的rowKey list，批量删除指定的记录
    */
  @Test
  @TestStep(step = 5, desc = "testHbaseDeleteList")
  def testHbaseDeleteList: Unit = {
    this.putData

    val rowKeyList = Seq(1.toString, 2.toString, 5.toString, 8.toString)
    this.fire.hbaseDeleteList(this.tableName1, rowKeyList)

    val getList = rowKeyList.map(rowKey => HBaseConnector.buildGet(rowKey))
    val result = this.fire.hbaseGetList(this.tableName1, classOf[Student], getList)
    assert(result.isEmpty)
  }

  /**
    * 根据指定的rowKey rdd，批量删除指定的记录
    */
  @Test
  @TestStep(step = 6, desc = "testHbasePutList")
  def testHBaseDeleteRDD: Unit = {
    this.putData

    val rowKeyList = Seq(1.toString, 2.toString, 3.toString, 4.toString, 5.toString, 6.toString, 7.toString, 8.toString, 9.toString, 10.toString)
    val rowKeyRDD = this.fire.createRDD(rowKeyList, 2)
    rowKeyRDD.hbaseDeleteRDD(this.tableName1)

    val getList = rowKeyList.map(rowKey => HBaseConnector.buildGet(rowKey))
    val result = this.fire.hbaseGetList(this.tableName1, classOf[Student], getList)
    assert(result.isEmpty)
  }

  /**
    * 根据指定的rowKey dataset，批量删除指定的记录
    */
  @Test
  @TestStep(step = 6, desc = "testHbaseDeleteDS")
  def testHbaseDeleteDS: Unit = {
    this.putData

    val rowKeyList = Seq(1.toString, 2.toString, 5.toString, 8.toString)
    val rowKeyDS = this.fire.createDataset(rowKeyList)(Encoders.STRING)
    rowKeyDS.hbaseDeleteDS(this.tableName1)

    val getList = rowKeyList.map(rowKey => HBaseConnector.buildGet(rowKey))
    val result = this.fire.hbaseGetList(this.tableName1, classOf[Student], getList)
    assert(result.isEmpty)
  }

  /**
   * 测试多版本
   */
  @Test
  @TestStep(step = 7, desc = "testMultiVersion")
  def testMultiVersion: Unit = {
    val studentList = StudentMulti.newStudentMultiList()
    val studentDF = this.fire.createDataFrame(studentList, classOf[StudentMulti])
    studentDF.hbasePutDF(this.tableName2, classOf[StudentMulti])

    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.fire.createRDD(getList, 3)
    // get到的结果以dataframe形式返回
    val resultDF = this.fire.hbaseGetDF(this.tableName2, classOf[StudentMulti], getRDD)
    assert(resultDF.count() == 6)

    val dataFrame = this.fire.hbaseScanDF2(this.tableName2, classOf[StudentMulti], "2", "6")
    assert(dataFrame.count() == 4)
    dataFrame.show()
  }
}
