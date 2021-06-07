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

package com.zto.fire.spark.ext.provider

import com.zto.fire._
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.spark.connector.HBaseSparkBridge
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.ClassTag

/**
 * 为扩展层提供HBaseConnector相关API
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-23 17:39
 */
trait HBaseConnectorProvider extends SparkProvider {

  /**
   * Scan指定HBase表的数据，并映射为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): DataFrame = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDF(tableName, clazz, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为DataFrame
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @param clazz
   *                目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): DataFrame = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDF2(tableName, clazz, startRow, stopRow)
  }

  /**
   * Scan指定HBase表的数据，并映射为Dataset
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): Dataset[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDS[T](tableName, clazz, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为Dataset
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @param clazz
   *                目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): Dataset[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDS2[T](tableName, clazz, startRow, stopRow)
  }

  /**
   * 使用hbase java api方式插入一个集合的数据到hbase表中
   *
   * @param tableName
   * hbase表名
   * @param seq
   * HBaseBaseBean的子类集合
   */
  def hbasePutList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T], keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutList[T](tableName, seq)
  }

  /**
   * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   */
  def hbasePutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], keyNum: Int = 1): Unit = {
    rdd.hbasePutRDD[T](tableName, keyNum)
  }

  /**
   * 使用Java API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param df
   * DataFrame
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hbasePutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, df: DataFrame, clazz: Class[E], keyNum: Int = 1): Unit = {
    df.hbasePutDF(tableName, clazz, keyNum)
  }

  /**
   * 使用Java API的方式将Dataset中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hbasePutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], clazz: Class[E], keyNum: Int = 1): Unit = {
    dataset.hbasePutDS[E](tableName, clazz, keyNum)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * HBase scan对象
   * @return
   */
  def hbaseScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): RDD[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanRDD(tableName, clazz, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowKey开始位置
   * @param stopRow
   * rowKey结束位置
   * 目标类型
   * @return
   */
  def hbaseScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): RDD[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanRDD(tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
   * Scan指定HBase表的数据，并映射为List
   *
   * @param tableName
   * HBase表名
   * @param scan
   * hbase scan对象
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseScanList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanList[T](tableName, clazz, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为List
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @param clazz
   *                目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseScanList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanList2[T](tableName, clazz, startRow, stopRow)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseGetRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], rdd: RDD[String], keyNum: Int = 1): RDD[T] = {
    rdd.hbaseGetRDD(tableName, clazz, keyNum)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseGetDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], rdd: RDD[String], keyNum: Int = 1): DataFrame = {
    rdd.hbaseGetDF(tableName, clazz, keyNum)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 目标类型
   * @tparam T
   * 目标类型
   * @return
   */
  def hbaseGetDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], rdd: RDD[String], keyNum: Int = 1): Dataset[T] = {
    rdd.hbaseGetDS[T](tableName, clazz, keyNum)
  }

  /**
   * 根据rowKey查询数据，并转为List[T]
   *
   * @param tableName
   * hbase表名
   * @param seq
   * rowKey集合
   * @param clazz
   * 目标类型
   * @return
   * List[T]
   */
  def hbaseGetList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], seq: Seq[Get], keyNum: Int = 1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetList[T](tableName, clazz, seq)
  }

  /**
   * 根据rowKey查询数据，并转为List[T]
   *
   * @param tableName
   * hbase表名
   * @param seq
   * rowKey集合
   * @param clazz
   * 目标类型
   * @return
   * List[T]
   */
  def hbaseGetList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], seq: Seq[String], keyNum: Int = 1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetList2[T](tableName, clazz, seq)
  }

  /**
   * 根据rowKey集合批量删除记录
   *
   * @param tableName
   * hbase表名
   * @param rowKeys
   * rowKey集合
   */
  def hbaseDeleteList(tableName: String, rowKeys: Seq[String], keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbaseDeleteList(tableName, rowKeys)
  }

  /**
   * 根据RDD[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   * @param rowKeyRDD
   * rowKey的rdd集合
   */
  def hbaseDeleteRDD(tableName: String, rowKeyRDD: RDD[String], keyNum: Int = 1): Unit = {
    rowKeyRDD.hbaseDeleteRDD(tableName, keyNum)
  }

  /**
   * 根据Dataset[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   */
  def hbaseDeleteDS(tableName: String, dataset: Dataset[String], keyNum: Int = 1): Unit = {
    dataset.hbaseDeleteDS(tableName, keyNum)
  }
}
