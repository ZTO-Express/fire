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
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.BaseSparkCore
import org.apache.spark.sql.{Encoders, Row}
import org.junit.Test

/**
  * 测试基于hadoop的方式读写HBase
  *
  * @author ChengLong
  * @date 2022-05-11 15:26:10 
  * @since 2.2.2
  */
@HBase("test")
@HBase2(cluster = "test", scanPartitions = 3)
class HBaseHadoopUnitTest extends BaseSparkCore with HBaseBaseTester {

  /**
   * 基于saveAsNewAPIHadoopDataset封装，将rdd数据保存到hbase中
   */
  @Test
  @TestStep(step = 1, desc = "testHbaseHadoopPutRDD")
  def testHbaseHadoopPutRDD: Unit = {
    val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
    this.fire.hbaseHadoopPutRDD(this.tableName1, studentRDD, keyNum = 2)
    this.assertScan
  }

  /**
   * 基于saveAsNewAPIHadoopDataset封装，将DataFrame数据保存到hbase中
   */
  @Test
  @TestStep(step = 2, desc = "testHbaseHadoopPutDF")
  def testHbaseHadoopPutDF: Unit = {
    val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDF = this.fire.createDataFrame(studentRDD, classOf[Student])
    this.fire.hbaseHadoopPutDF(this.tableName1, studentDF, classOf[Student])
    this.assertScan
  }

  /**
   * 基于saveAsNewAPIHadoopDataset封装，将Dataset数据保存到hbase中
   */
  @Test
  @TestStep(step = 3, desc = "testHbaseHadoopPutDS")
  def testHbaseHadoopPutDS: Unit = {
    val studentDS = this.fire.createDataset(Student.newStudentList())(Encoders.bean(classOf[Student]))
    this.fire.hbaseHadoopPutDS(this.tableName1, studentDS)
    this.assertScan
  }

  /**
   * 基于saveAsNewAPIHadoopDataset封装，将不是HBaseBaseBean结构对应的DataFrame保存到hbase中
   * 注：此方法与hbaseHadoopPutDF不同之处在于，它不强制要求该DataFrame一定要与HBaseBaseBean的子类对应
   * 但需要指定rowKey的构建规则，相对与hbaseHadoopPutDF来说，少了中间的两次转换，性能会更高
   */
  @Test
  @TestStep(step = 4, desc = "testHbaseHadoopPutDFRow")
  def testHbaseHadoopPutDFRow: Unit = {
    /**
     * 构建main_order rowkey
     */
    val buildRowKey = (row: Row) => {
      // 将id字段作为rowKey
      row.getAs("id").toString
    }

    val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
    this.fire.createDataFrame(studentRDD, classOf[Student]).createOrReplaceTempView("student")
    // 指定rowKey构建的函数
    this.fire.sql("select age,createTime,id,length,name,sex from student").hbaseHadoopPutDFRow(this.tableName1, buildRowKey)
    this.assertScan
  }

  /**
   * 断言scan结果
   */
  private def assertScan: Unit = {
    this.testHBaseHadoopScanRDD
    this.testHBaseHadoopScanDF
    this.testHBaseHadoopScanDS
  }

  /**
   * 使用Spark的方式scan海量数据，并将结果集映射为RDD
   */
  private def testHBaseHadoopScanRDD: Unit = {
    val studentRDD = this.fire.hbaseHadoopScanRDD2(this.tableName1, classOf[Student], "1", "6", keyNum = 2)
    assert(studentRDD.count() == 5)
  }

  /**
   * 使用Spark的方式scan海量数据，并将结果集映射为DataFrame
   */
  private def testHBaseHadoopScanDF: Unit = {
    val studentDF = this.fire.hbaseHadoopScanDF2(this.tableName1, classOf[Student], "1", "6")
    assert(studentDF.count() == 5)
    studentDF.show()
  }

  /**
   * 使用Spark的方式scan海量数据，并将结果集映射为Dataset
   */
  private def testHBaseHadoopScanDS: Unit = {
    val studentDS = this.fire.hbaseHadoopScanDS2(this.tableName1, classOf[Student], "1", "6")
    assert(studentDS.count() == 5)
  }
}
