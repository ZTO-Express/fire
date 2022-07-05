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
import com.zto.fire.core.anno.{HBase, HBase2}
import com.zto.fire.examples.bean.{Student, StudentMulti}
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.BaseSparkCore
import org.apache.hadoop.hbase.client.Get
import org.apache.spark.sql.{Encoders, Row}
import org.junit.Test

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

/**
  * 测试基于bulk的方式读写HBase
  *
  * @author ChengLong
  * @date 2022-05-11 15:01:10 
  * @since 2.2.2
  */
@HBase("test")
@HBase2(cluster = "test", scanPartitions = 3)
class HBaseBulkUnitTest extends BaseSparkCore with HBaseBaseTester {

  /**
   * 使用id作为rowKey
   */
  val buildStudentRowKey = (row: Row) => {
    row.getAs("id").toString
  }

  /**
   * 使用bulk的方式将rdd写入到hbase
   */
  @Test
  @TestStep(step = 1, desc = "testHbaseBulkPutRDD")
  def testHbaseBulkPutRDD: Unit = {
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    this.fire.hbaseBulkPutRDD(this.tableName1, rdd)
    this.assertResult
  }

  /**
   * 使用bulk的方式将DataFrame写入到hbase
   */
  @Test
  @TestStep(step = 2, desc = "testHbaseBulkPutDF")
  def testHbaseBulkPutDF: Unit = {
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDF = this.fire.createDataFrame(rdd, classOf[Student])
    this.fire.hbaseBulkPutDF(this.tableName1, studentDF, classOf[Student])
    this.assertResult
  }

  /**
   * 使用bulk的方式将Dataset写入到hbase
   */
  @Test
  @TestStep(step = 3, desc = "testHbaseBulkPutDS")
  def testHbaseBulkPutDS: Unit = {
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDataset = this.fire.createDataset(rdd)(Encoders.bean(classOf[Student]))
    this.fire.hbaseBulkPutDS(this.tableName1, studentDataset)
    this.assertResult
  }

  /**
   * 使用bulk方式批量删除指定的rowKey对应的数据
   */
  @Test
  @TestStep(step = 4, desc = "testHBaseBulkDeleteRDD")
  def testHBaseBulkDeleteRDD: Unit = {
    this.testHbaseBulkPutRDD
    val rowKeySeq = Seq(1.toString, 2.toString, 5.toString, 6.toString)
    this.fire.hbaseDeleteList(this.tableName1, rowKeySeq)
    val getList = rowKeySeq.map(rowKey => HBaseConnector.buildGet(rowKey))
    val result = this.fire.hbaseGetList(this.tableName1, classOf[Student], getList)
    assert(result.isEmpty)
  }

  /**
   * 使用bulk方式批量删除指定的rowKey对应的数据
   */
  @Test
  @TestStep(step = 5, desc = "testHBaseBulkDeleteDS")
  def testHBaseBulkDeleteDS: Unit = {
    this.testHbaseBulkPutRDD
    val rowKeySeq = Seq(1.toString, 2.toString, 5.toString, 6.toString)
    val rowKeyRdd = this.fire.createRDD(rowKeySeq, 2)
    this.fire.createDataset(rowKeyRdd)(Encoders.STRING).hbaseBulkDeleteDS(this.tableName1)
    val getList = rowKeySeq.map(rowKey => HBaseConnector.buildGet(rowKey))
    val result = this.fire.hbaseGetList(this.tableName1, classOf[Student], getList)
    assert(result.isEmpty)
  }

  /**
   * 通过查询结果断言是否正确
   */
  private def assertResult: Unit = {
    this.testHBaseBulkGetSeq
    this.testHBaseBulkGetRDD
    this.testHBaseBulkGetDF
    this.testHBaseBulkGetDS
    this.testHbaseBulkScanRDD
    this.testHbaseBulkScanDF
    this.testHbaseBulkScanDS
  }

  /**
   * 使用bulk方式根据rowKey集合获取数据，并将结果集以RDD形式返回
   */
  private def testHBaseBulkGetSeq: Unit = {
    val seq = Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString)
    val studentRDD = this.fire.hbaseBulkGetSeq(this.tableName1, seq, classOf[Student])
    assert(studentRDD.count() == 5)
  }

  /**
   * 使用bulk方式根据rowKey获取数据，并将结果集以RDD形式返回
   */
  private def testHBaseBulkGetRDD: Unit = {
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString), 2)
    val studentRDD = rowKeyRdd.hbaseBulkGetRDD(this.tableName1, classOf[Student], keyNum = 2)
    assert(studentRDD.count() == 5)
  }

  /**
   * 使用bulk方式根据rowKey获取数据，并将结果集以DataFrame形式返回
   */
  private def testHBaseBulkGetDF: Unit = {
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString, 111.toString), 2)
    val studentDF = this.fire.hbaseBulkGetDF(this.tableName1, rowKeyRdd, classOf[Student])
    assert(studentDF.count() == 5)

    val rowKeyRdd2 = this.fire.createRDD(Seq[String](), 2)
    val studentDF2 = this.fire.hbaseBulkGetDF(this.tableName1, rowKeyRdd2, classOf[Student])
    assert(studentDF2.count() == 0)
  }

  /**
   * 使用bulk方式根据rowKey获取数据，并将结果集以Dataset形式返回
   */
  private def testHBaseBulkGetDS: Unit = {
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString), 2)
    val studentDS2 = this.fire.hbaseBulkGetDS(this.tableName1, rowKeyRdd, classOf[Student])
    assert(studentDS2.count() == 5)
  }

  /**
   * 使用bulk方式进行scan，并将结果集映射为RDD
   */
  private def testHbaseBulkScanRDD: Unit = {
    val scanRDD = this.fire.hbaseBulkScanRDD2(this.tableName1, classOf[Student], "1", "6")
    assert(scanRDD.count() == 5)
  }

  /**
   * 使用bulk方式进行scan，并将结果集映射为DataFrame
   */
  private def testHbaseBulkScanDF: Unit = {
    val scanDF = this.fire.hbaseBulkScanDF2(this.tableName1, classOf[Student], "1", "6")
    assert(scanDF.count() == 5)
  }

  /**
   * 使用bulk方式进行scan，并将结果集映射为Dataset
   */
  private def testHbaseBulkScanDS: Unit = {
    val scanDS = this.fire.hbaseBulkScanDS(this.tableName1, classOf[Student], HBaseConnector.buildScan("1", "6"))
    assert(scanDS.count() == 5)
  }

}
