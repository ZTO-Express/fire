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
import com.zto.fire.spark.connector.HBaseBulkConnector
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * 为扩展层提供HBase bulk api
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:31
 */
trait HBaseBulkProvider extends SparkProvider {

  /**
   * scan数据，并转为RDD
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): RDD[T] = {
    HBaseBulkConnector.bulkScanRDD(tableName, clazz, scan, keyNum)
  }

  /**
   * scan数据，并转为RDD
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * 开始
   * @param stopRow
   * 结束
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): RDD[T] = {
    HBaseBulkConnector.bulkScanRDD2(tableName, clazz, startRow, stopRow, keyNum)
  }

  /**
   * 使用bulk方式scan数据，并转为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): DataFrame = {
    val rdd = HBaseBulkConnector.bulkScanRDD(tableName, clazz, scan, keyNum)
    this.spark.createDataFrame(rdd, clazz)
  }

  /**
   * 使用bulk方式scan数据，并转为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * 开始
   * @param stopRow
   * 结束
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): DataFrame = {
    this.hbaseBulkScanDF[T](tableName, clazz, HBaseConnector.buildScan(startRow, stopRow), keyNum)
  }

  /**
   * 使用bulk方式scan数据，并转为Dataset
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): Dataset[T] = {
    val rdd = HBaseBulkConnector.bulkScanRDD(tableName, clazz, scan, keyNum)
    this.spark.createDataset(rdd)(Encoders.bean(clazz))
  }

  /**
   * 使用bulk方式scan数据，并转为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * 开始
   * @param stopRow
   * 结束
   * @param clazz
   * 对应的返回值类型
   * @return
   * clazz类型的rdd
   */
  def hbaseBulkScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): Dataset[T] = {
    this.hbaseBulkScanDS[T](tableName, clazz, HBaseConnector.buildScan(startRow, stopRow), keyNum)
  }

  /**
   * 使用bulk方式批量插入数据
   *
   * @param tableName
   * HBase表名
   * 数据集合，继承自HBaseBaseBean
   */
  def hbaseBulkPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], keyNum: Int = 1): Unit = {
    rdd.hbaseBulkPutRDD(tableName, keyNum)
  }

  /** hbaseInsertList
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def hbaseBulkPutDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[T], keyNum: Int = 1): Unit = {
    dataFrame.hbaseBulkPutDF[T](tableName, clazz, keyNum)
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param dataset
   * dataFrame实例，数类型需继承自HBaseBaseBean
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def hbaseBulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataset: Dataset[T], keyNum: Int = 1): Unit = {
    dataset.hbaseBulkPutDS[T](tableName, keyNum)
  }

  /**
   * DStrea数据实时写入
   *
   * @param tableName
   * HBase表名
   */
  def hbaseBulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dstream: DStream[T], keyNum: Int = 1): Unit = {
    dstream.hbaseBulkPutStream[T](tableName, keyNum)
  }

  /**
   * 根据RDD[String]批量删除
   *
   * @param tableName
   * HBase表名
   * @param rowKeyRDD
   * 装有rowKey的rdd集合
   */
  def hbaseBulkDeleteRDD(tableName: String, rowKeyRDD: RDD[String], keyNum: Int = 1): Unit = {
    rowKeyRDD.hbaseBulkDeleteRDD(tableName, keyNum)
  }

  /**
   * 根据Dataset[String]批量删除，Dataset是rowkey的集合
   * 类型为String
   *
   * @param tableName
   * HBase表名
   */
  def hbaseBulkDeleteDS(tableName: String, dataSet: Dataset[String], keyNum: Int = 1): Unit = {
    dataSet.hbaseBulkDeleteDS(tableName, keyNum)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   * 内部实现是将rowkey集合转为RDD[String]，推荐在数据量较大
   * 时使用。数据量较小请优先使用HBaseOper
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 具体类型
   * @param seq
   * rowKey集合
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def hbaseBulkGetSeq[E <: HBaseBaseBean[E] : ClassTag](tableName: String, seq: Seq[String], clazz: Class[E], keyNum: Int = 1): RDD[E] = {
    HBaseBulkConnector.bulkGetSeq[E](tableName, seq, clazz, keyNum)
  }

  /**
   * 根据rowKey集合批量获取数据
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 获取后的记录转换为目标类型
   * @return
   * 结果集
   */
  def hbaseBulkGetRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[E], keyNum: Int = 1): RDD[E] = {
    rowKeyRDD.hbaseBulkGetRDD[E](tableName, clazz, keyNum)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def hbaseBulkGetDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[E], keyNum: Int = 1): DataFrame = {
    rowKeyRDD.hbaseBulkGetDF[E](tableName, clazz, keyNum)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def hbaseBulkGetDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[E], keyNum: Int = 1): Dataset[E] = {
    rowKeyRDD.hbaseBulkGetDS[E](tableName, clazz, keyNum)
  }
}
