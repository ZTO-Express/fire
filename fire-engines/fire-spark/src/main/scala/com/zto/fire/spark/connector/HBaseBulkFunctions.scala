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

package com.zto.fire.spark.connector

import com.zto.fire.hbase.bean.HBaseBaseBean
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * HBase Bulk api库
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 15:46
 */
trait HBaseBulkFunctions {
  /**
   * 根据RDD[String]批量删除，rdd是rowkey的集合
   * 类型为String
   *
   * @param rdd
   * 类型为String的RDD数据集
   * @param tableName
   * HBase表名
   */
  def bulkDeleteRDD(tableName: String, rdd: RDD[String], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkDeleteRDD(tableName, rdd)
  }

  /**
   * 根据Dataset[String]批量删除，Dataset是rowkey的集合
   * 类型为String
   *
   * @param dataset
   * 类型为String的Dataset集合
   * @param tableName
   * HBase表名
   */
  def bulkDeleteDS(tableName: String, dataset: Dataset[String], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkDeleteDS(tableName, dataset)
  }

  /**
   * 指定rowkey集合，进行批量删除操作内部会将这个集合转为RDD
   * 推荐在较大量数据时使用，小数据量的删除操作仍推荐使用HBaseConnector
   *
   * @param tableName
   * HBase表名
   * @param seq
   * 待删除的rowKey集合
   */
  def bulkDeleteList(tableName: String, seq: Seq[String], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkDeleteList(tableName, seq)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * rowKey集合，类型为RDD[String]
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def bulkGetRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E], keyNum: Int = 1): RDD[E] = {
    HBaseBulkConnector(keyNum = keyNum).bulkGetRDD[E](tableName, rdd, clazz)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * rowKey集合，类型为RDD[String]
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def bulkGetDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E], keyNum: Int = 1): DataFrame = {
    HBaseBulkConnector(keyNum = keyNum).bulkGetDF[E](tableName, rdd, clazz)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * rowKey集合，类型为RDD[String]
   * @param clazz
   * 获取后的记录转换为目标类型（自定义的JavaBean类型）
   * @tparam E
   * 自定义JavaBean类型，必须继承自HBaseBaseBean
   * @return
   * 自定义JavaBean的对象结果集
   */
  def bulkGetDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E], keyNum: Int = 1): Dataset[E] = {
    HBaseBulkConnector(keyNum = keyNum).bulkGetDS[E](tableName, rdd, clazz)
  }

  /**
   * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
   * 内部实现是将rowkey集合转为RDD[String]，推荐在数据量较大
   * 时使用。数据量较小请优先使用HBaseConnector
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
  def bulkGetSeq[E <: HBaseBaseBean[E] : ClassTag](tableName: String, seq: Seq[String], clazz: Class[E], keyNum: Int = 1): RDD[E] = {
    HBaseBulkConnector(keyNum = keyNum).bulkGetSeq[E](tableName, seq, clazz)
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param rdd
   * 数据集合，数类型需继承自HBaseBaseBean
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def bulkPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkPutRDD[T](tableName, rdd)
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入。如果数据量
   * 较大，推荐使用。数据量过小则推荐使用HBaseConnector
   *
   * @param tableName
   * HBase表名
   * @param seq
   * 数据集，类型为HBaseBaseBean的子类
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   */
  def bulkPutSeq[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkPutSeq[T](tableName, seq)
  }

  /**
   * 定制化scan设置后从指定的表中scan数据
   * 并将scan到的结果集映射为自定义JavaBean对象
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @param clazz
   * 自定义JavaBean的Class对象
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   * @return
   * scan获取到的结果集，类型为RDD[T]
   */
  def bulkScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): RDD[T] = {
    HBaseBulkConnector(keyNum = keyNum).bulkScanRDD[T](tableName, clazz, scan)
  }

  /**
   * 指定startRow和stopRow后自动创建scan对象完成数据扫描
   * 并将scan到的结果集映射为自定义JavaBean对象
   *
   * @param tableName
   * HBase表名
   * @param startRow
   * rowkey的起始
   * @param stopRow
   * rowkey的结束
   * @param clazz
   * 自定义JavaBean的Class对象
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   * @return
   * scan获取到的结果集，类型为RDD[T]
   */
  def bulkScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): RDD[T] = {
    HBaseBulkConnector(keyNum = keyNum).bulkScanRDD2[T](tableName, clazz, startRow, stopRow)
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @param dataFrame
   * dataFrame实例，数类型需继承自HBaseBaseBean
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def bulkPutDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[T], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkPutDF[T](tableName, dataFrame, clazz)
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
  def bulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataset: Dataset[T], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkPutDS[T](tableName, dataset)
  }

  /**
   * 用于已经映射为指定类型的DStream实时
   * 批量写入至HBase表中
   *
   * @param tableName
   * HBase表名
   * @param dstream
   * 类型为自定义JavaBean的DStream流
   * @tparam T
   * 对象类型必须是HBaseBaseBean的子类
   */
  def bulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dstream: DStream[T], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).bulkPutStream[T](tableName, dstream)
  }

  /**
   * 以spark 方式批量将rdd数据写入到hbase中
   *
   * @param rdd
   * 类型为HBaseBaseBean子类的rdd
   * @param tableName
   * hbase表名
   * @tparam T
   * 数据类型
   */
  def hadoopPut[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).hadoopPut[T](tableName, rdd)
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hadoopPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[E], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).hadoopPutDF[E](tableName, dataFrame, clazz)
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param dataset
   * JavaBean类型，待插入到hbase的数据集
   */
  def hadoopPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).hadoopPutDS[E](tableName, dataset)
  }

  /**
   * 以spark 方式批量将DataFrame数据写入到hbase中
   * 注：此方法与hbaseHadoopPutDF不同之处在于，它不强制要求该DataFrame一定要与HBaseBaseBean的子类对应
   * 但需要指定rowKey的构建规则，相对与hbaseHadoopPutDF来说，少了中间的两次转换，性能会更高
   *
   * @param df
   * spark的DataFrame
   * @param tableName
   * hbase表名
   * @tparam T
   * JavaBean类型
   */
  def hadoopPutDFRow[T <: HBaseBaseBean[T] : ClassTag](tableName: String, df: DataFrame, buildRowKey: (Row) => String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector(keyNum = keyNum).hadoopPutDFRow[T](tableName, df, buildRowKey)
  }
}
