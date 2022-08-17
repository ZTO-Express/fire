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
import com.zto.fire.core.anno.connector.{HBase, HBase2}
import com.zto.fire.examples.bean.Student
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkCore
import org.apache.spark.sql.{Encoders, Row}


/**
  * 本示例用于演示spark中使用bulk api完成HBase的读写
  * 注：bulk api相较于java api，在速度上会更快，但目前暂不支持多版本读写
  *
  * @author ChengLong 2019-5-18 09:20:52
  *  @contact Fire框架技术交流群（钉钉）：35373471
  */
@HBase("test")
@HBase2(cluster = "test", scanPartitions = 3, storageLevel = "DISK_ONLY")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HBaseBulkTest extends SparkCore {
  private val tableName3 = "fire_test_3"
  private val tableName5 = "fire_test_5"

  /**
    * 使用id作为rowKey
    */
  val buildStudentRowKey = (row: Row) => {
    row.getAs("id").toString
  }

  /**
    * 使用bulk的方式将rdd写入到hbase
    */
  def testHbaseBulkPutRDD: Unit = {
    // 方式一：将rdd的数据写入到hbase中，rdd类型必须为HBaseBaseBean的子类
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    // rdd.hbaseBulkPutRDD(this.tableName2)
    // 方式二：使用this.fire.hbaseBulkPut将rdd中的数据写入到hbase
    this.fire.hbaseBulkPutRDD(this.tableName5, rdd)

    // 第二个参数指定false表示不插入为null的字段到hbase中
    // rdd.hbaseBulkPutRDD(this.tableName2, insertEmpty = false)
    // 第三个参数为true表示以多版本json格式写入
    // rdd.hbaseBulkPutRDD(this.tableName3, false, true)
  }

  /**
    * 使用bulk的方式将DataFrame写入到hbase
    */
  def testHbaseBulkPutDF: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDF = this.fire.createDataFrame(rdd, classOf[Student])
    // insertEmpty=false表示为空的字段不插入
    studentDF.hbaseBulkPutDF(this.tableName3, classOf[Student], keyNum = 2)
    // 方式二：
    // this.fire.hbaseBulkPutDF(this.tableName2, studentDF, classOf[Student])
  }

  /**
    * 使用bulk的方式将Dataset写入到hbase
    */
  def testHbaseBulkPutDS: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDataset = this.fire.createDataset(rdd)(Encoders.bean(classOf[Student]))
    // multiVersion=true表示以多版本形式插入
    studentDataset.hbaseBulkPutDS(this.tableName5)
    // 方式二：
    // this.fire.hbaseBulkPutDS(this.tableName3, studentDataset)
  }

  /**
    * 使用bulk方式根据rowKey集合获取数据，并将结果集以RDD形式返回
    */
  def testHBaseBulkGetSeq: Unit = {
    println("===========testHBaseBulkGetSeq===========")
    // 方式一：使用rowKey集合读取hbase中的数据
    val seq = Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString)
    val studentRDD = this.fire.hbaseBulkGetSeq(this.tableName5, seq, classOf[Student])
    studentRDD.foreach(println)
    // 方式二：使用this.fire.hbaseBulkGetRDD
    /*val studentRDD2 = this.fire.hbaseBulkGetSeq(this.tableName2, seq, classOf[Student])
    studentRDD2.foreach(println)*/
  }

  /**
    * 使用bulk方式根据rowKey获取数据，并将结果集以RDD形式返回
    */
  def testHBaseBulkGetRDD: Unit = {
    println("===========testHBaseBulkGetRDD===========")
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString), 2)
    val studentRDD = rowKeyRdd.hbaseBulkGetRDD(this.tableName3, classOf[Student], keyNum = 2)
    studentRDD.foreach(println)
    // 方式二：使用this.fire.hbaseBulkGetRDD
    // val studentRDD2 = this.fire.hbaseBulkGetRDD(this.tableName2, rowKeyRdd, classOf[Student])
    // studentRDD2.foreach(println)
  }

  /**
    * 使用bulk方式根据rowKey获取数据，并将结果集以DataFrame形式返回
    */
  def testHBaseBulkGetDF: Unit = {
    println("===========testHBaseBulkGetDF===========")
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString), 2)
    val studentDF = rowKeyRdd.hbaseBulkGetDF(this.tableName5, classOf[Student])
    studentDF.show(100, false)
    // 方式二：使用this.fire.hbaseBulkGetDF
    val studentDF2 = this.fire.hbaseBulkGetDF(this.tableName5, rowKeyRdd, classOf[Student])
    studentDF2.show(100, false)
  }

  /**
    * 使用bulk方式根据rowKey获取数据，并将结果集以Dataset形式返回
    */
  def testHBaseBulkGetDS: Unit = {
    println("===========testHBaseBulkGetDS===========")
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString), 2)
    val studentDS = rowKeyRdd.hbaseBulkGetDS(this.tableName5, classOf[Student])
    studentDS.show(100, false)
    // 方式二：使用this.fire.hbaseBulkGetDF
    // val studentDS2 = this.fire.hbaseBulkGetDS(this.tableName2, rowKeyRdd, classOf[Student])
    // studentDS2.show(100, false)
  }

  /**
    * 使用bulk方式进行scan，并将结果集映射为RDD
    */
  def testHbaseBulkScanRDD: Unit = {
    println("===========testHbaseBulkScanRDD===========")
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为RDD[Student]
    val scanRDD = this.fire.hbaseBulkScanRDD2(this.tableName5, classOf[Student], "1", "6")
    scanRDD.foreach(println)
  }

  /**
    * 使用bulk方式进行scan，并将结果集映射为DataFrame
    */
  def testHbaseBulkScanDF: Unit = {
    println("===========testHbaseBulkScanDF===========")
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为DataFrame
    val scanDF = this.fire.hbaseBulkScanDF2(this.tableName5, classOf[Student], "1", "6")
    scanDF.show(100, false)
  }

  /**
    * 使用bulk方式进行scan，并将结果集映射为Dataset
    */
  def testHbaseBulkScanDS: Unit = {
    println("===========testHbaseBulkScanDS===========")
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为Dataset[Student]
    val scanDS = this.fire.hbaseBulkScanDS(this.tableName5, classOf[Student], HBaseConnector.buildScan("1", "6"))
    scanDS.show(100, false)
  }

  /**
    * 使用bulk方式批量删除指定的rowKey对应的数据
    */
  def testHBaseBulkDeleteRDD: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 5.toString, 6.toString), 2)
    // 根据rowKey删除
    rowKeyRdd.hbaseBulkDeleteRDD(this.tableName5)

    // 方式二：使用this.fire.hbaseBulkDeleteRDD
    // this.fire.hbaseBulkDeleteRDD(this.tableName1, rowKeyRdd)
  }

  /**
    * 使用bulk方式批量删除指定的rowKey对应的数据
    */
  def testHBaseBulkDeleteDS: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 5.toString, 6.toString), 2)
    // 根据rowKey删除
    this.fire.createDataset(rowKeyRdd)(Encoders.STRING).hbaseBulkDeleteDS(this.tableName5)

    // 方式二：使用this.fire.hbaseBulkDeleteDS
    // this.fire.hbaseBulkDeleteDS(this.tableName1, rowKeyRdd)
  }


  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    this.testHBaseBulkDeleteRDD
    HBaseConnector.truncateTable(this.tableName3, keyNum = 2)
    HBaseConnector.truncateTable(this.tableName5)
    // this.testHBaseBulkDeleteDS

    // this.testHbaseBulkPutRDD
    this.testHbaseBulkPutDF
    this.testHbaseBulkPutDS

    println("=========get========")
    this.testHBaseBulkGetRDD
    this.testHBaseBulkGetDF
    this.testHBaseBulkGetDS
    this.testHBaseBulkGetSeq

    println("=========scan========")
    this.testHbaseBulkScanRDD
    this.testHbaseBulkScanDF
    this.testHbaseBulkScanDS
  }
}
