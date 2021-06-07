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
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.spark.connector.HBaseSparkBridge
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.reflect.ClassTag

/**
 * 为扩展层提供spark方式的HBase操作API
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:41
 */
trait HBaseHadoopProvider extends SparkProvider {

  /**
   * 使用Spark API的方式将RDD中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   */
  def hbaseHadoopPutRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[E], keyNum: Int = 1): Unit = {
    rdd.hbaseHadoopPutRDD(tableName, keyNum)
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hbaseHadoopPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[E], keyNum: Int = 1): Unit = {
    dataFrame.hbaseHadoopPutDF(tableName, clazz, keyNum)
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param dataset
   * JavaBean类型，待插入到hbase的数据集
   */
  def hbaseHadoopPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], keyNum: Int = 1): Unit = {
    dataset.hbaseHadoopPutDS[E](tableName, keyNum)
  }

  /**
   * 以spark 方式批量将DataFrame数据写入到hbase中
   *
   * @param tableName
   * hbase表名
   * @tparam T
   * JavaBean类型
   */
  def hbaseHadoopPutDFRow[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataFrame: DataFrame, buildRowKey: (Row) => String, keyNum: Int = 1): Unit = {
    dataFrame.hbaseHadoopPutDFRow[T](tableName, buildRowKey, keyNum)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanRS(tableName: String, scan: Scan, keyNum: Int = 1): RDD[(ImmutableBytesWritable, Result)] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanRS(tableName, scan)
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
  def hbaseHadoopScanRS2(tableName: String, startRow: String, stopRow: String, keyNum: Int = 1): RDD[(ImmutableBytesWritable, Result)] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanRS2(tableName, startRow, stopRow)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(T]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): RDD[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanRDD[T](tableName, clazz, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[T]
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
  def hbaseHadoopScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): RDD[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanRDD2[T](tableName, clazz, startRow, stopRow)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(T]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], keyNum: Int = 1): DataFrame = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanDF[T](tableName, clazz, scan)
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
  def hbaseHadoopScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): DataFrame = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanDF2[T](tableName, clazz, startRow, stopRow)
  }

  /**
   * Scan指定HBase表的数据，并映射为RDD[(T]
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * 目标类型
   * @return
   */
  def hbaseHadoopScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan, keyNum: Int = 1): Dataset[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanDS[T](tableName, clazz, scan)
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
  def hbaseHadoopScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String, keyNum: Int = 1): Dataset[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseHadoopScanDS2[T](tableName, clazz, startRow, stopRow)
  }
}
