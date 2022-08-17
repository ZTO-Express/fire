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
import com.zto.fire.common.anno.Config
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkCore
import org.apache.spark.sql.{Encoders, Row}

/**
  * 本示例演示Spark提供的hbase api封装后的使用
  * 注：使用Spark写hbase的方式适用于海量数据离线写
  *
  * @author ChengLong 2019-5-9 09:37:25
  * @contact Fire框架技术交流群（钉钉）：35373471
  */
@Config(
  """
    |# 用于区分不同的hbase集群: batch/streaming/old
    |spark.hbase.cluster                =       test
    |spark.hbase.cluster2               =       test
    |# 通过HBase scan后repartition的分区数，需根据scan后的数据量做配置
    |spark.fire.hbase.scan.partitions   =       3
    |spark.fire.hbase.storage.level     =       DISK_ONLY
    |""")
object HBaseHadoopTest extends SparkCore {
  private val tableName6 = "fire_test_6"
  private val tableName7 = "fire_test_7"

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将rdd数据保存到hbase中
    */
  def testHbaseHadoopPutRDD: Unit = {
    val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
    this.fire.hbaseHadoopPutRDD(this.tableName6, studentRDD, keyNum = 2)
    // 方式二：直接基于rdd进行方法调用
    // studentRDD.hbaseHadoopPutRDD(this.tableName1)
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将DataFrame数据保存到hbase中
    */
  def testHbaseHadoopPutDF: Unit = {
    val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDF = this.fire.createDataFrame(studentRDD, classOf[Student])
    // 由于DataFrame相较于Dataset和RDD是弱类型的数据集合，所以需要传递具体的类型classOf[Type]
    this.fire.hbaseHadoopPutDF(this.tableName7, studentDF, classOf[Student])
    // 方式二：基于DataFrame进行方法调用
    // studentDF.hbaseHadoopPutDF(this.tableName3, classOf[Student])
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将Dataset数据保存到hbase中
    */
  def testHbaseHadoopPutDS: Unit = {
    val studentDS = this.fire.createDataset(Student.newStudentList())(Encoders.bean(classOf[Student]))
    this.fire.hbaseHadoopPutDS(this.tableName7, studentDS)
    // 方式二：基于DataFrame进行方法调用
    // studentDS.hbaseHadoopPutDS(this.tableName3)
  }

  /**
    * 基于saveAsNewAPIHadoopDataset封装，将不是HBaseBaseBean结构对应的DataFrame保存到hbase中
    * 注：此方法与hbaseHadoopPutDF不同之处在于，它不强制要求该DataFrame一定要与HBaseBaseBean的子类对应
    * 但需要指定rowKey的构建规则，相对与hbaseHadoopPutDF来说，少了中间的两次转换，性能会更高
    */
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
    sql("select age,createTime,id,length,name,sex from student").hbaseHadoopPutDFRow(this.tableName7, buildRowKey)
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为RDD
    */
  def testHBaseHadoopScanRDD: Unit = {
    println("===========testHBaseHadoopScanRDD===========")
    val studentRDD = this.fire.hbaseHadoopScanRDD2(this.tableName6, classOf[Student], "1", "6", keyNum = 2)
    studentRDD.printEachPartition
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为DataFrame
    */
  def testHBaseHadoopScanDF: Unit = {
    println("===========testHBaseHadoopScanDF===========")
    val studentDF = this.fire.hbaseHadoopScanDF2(this.tableName7, classOf[Student], "1", "6")
    studentDF.show(100, false)
  }

  /**
    * 使用Spark的方式scan海量数据，并将结果集映射为Dataset
    */
  def testHBaseHadoopScanDS: Unit = {
    println("===========testHBaseHadoopScanDS===========")
    val studentDS = this.fire.hbaseHadoopScanDS2(this.tableName7, classOf[Student], "1", "6")
    studentDS.show(100, false)
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    HBaseConnector.truncateTable(this.tableName6, keyNum = 2)
    HBaseConnector.truncateTable(this.tableName7)
    this.testHbaseHadoopPutRDD
    // this.testHbaseHadoopPutDF
    // this.testHbaseHadoopPutDS
    this.testHbaseHadoopPutDFRow

    this.testHBaseHadoopScanRDD
    this.testHBaseHadoopScanDF
    this.testHBaseHadoopScanDS
  }
}
