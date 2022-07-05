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

package com.zto.fire.spark.ext.core

import com.zto.fire._
import com.zto.fire.common.util.Logging
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.connector.{HBaseBulkConnector, HBaseSparkBridge}
import com.zto.fire.spark.util.SparkUtils
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger

import scala.collection.mutable.ListBuffer
import scala.reflect._

/**
 * Dataset扩展
 *
 * @param dataset
 * dataset对象
 * @author ChengLong 2019-5-18 11:02:56
 */
class DatasetExt[T: ClassTag](dataset: Dataset[T]) extends Logging {

  /**
   * 用于检查当前Dataset是否为空
   *
   * @return
   * true: 为空 false：不为空
   */
  def isEmpty: Boolean = dataset.rdd.isEmpty()

  /**
   * 用于检查当前Dataset是否不为空
   *
   * @return
   * true: 不为空 false：为空
   */
  def isNotEmpty: Boolean = !this.isEmpty

  /**
   * 打印Dataset的值
   *
   * @param lines
   * 打印的行数
   * @return
   */
  def showString(lines: Int = 1000): String = {
    val showLines = if (lines <= 1000) lines else 1000
    val showStringMethod = dataset.getClass.getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
    showStringMethod.invoke(dataset, Integer.valueOf(showLines), Integer.valueOf(Int.MaxValue), java.lang.Boolean.valueOf(false)).toString
  }

  /**
   * 批量写入，将自定义的JavaBean数据集批量并行写入
   * 到HBase的指定表中。内部会将自定义JavaBean的相应
   * 字段一一映射为Put对象，并完成一次写入
   *
   * @param tableName
   * HBase表名
   * @tparam T
   * 数据类型为HBaseBaseBean的子类
   */
  def hbaseBulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.bulkPutDS[T](tableName, dataset.asInstanceOf[Dataset[T]], keyNum)
  }

  /**
   * 根据Dataset[String]批量删除，Dataset是rowkey的集合
   * 类型为String
   *
   * @param tableName
   * HBase表名
   */
  def hbaseBulkDeleteDS(tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.bulkDeleteDS(tableName, dataset.asInstanceOf[Dataset[String]], keyNum)
  }

  /**
   * 根据Dataset[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   */
  def hbaseDeleteDS(tableName: String, keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbaseDeleteDS(tableName, dataset.asInstanceOf[Dataset[String]])
  }

  /**
   * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   */
  def hbaseHadoopPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.hadoopPutDS[T](tableName, dataset.asInstanceOf[Dataset[T]], keyNum)
  }

  /**
   * 使用Java API的方式将Dataset中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param clazz
   * JavaBean类型，为HBaseBaseBean的子类
   */
  def hbasePutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutDS[E](tableName, clazz, dataset.asInstanceOf[Dataset[E]])
  }

  /**
   * 清空RDD的缓存
   */
  def uncache: Unit = {
    dataset.unpersist
  }

  /**
   * 将当前Dataset记录打印到控制台
   */
  def print(outputMode: String = "append", trigger: Trigger = null, numRows: Int = 20, truncate: Boolean = true): Dataset[T] = {
    if (dataset.isStreaming) {
      val tmpStream = dataset.writeStream.outputMode(outputMode).option("numRows", numRows).option("truncate", truncate).format("console")
      if (trigger != null) tmpStream.trigger(trigger)
      tmpStream.start
    } else {
      dataset.show(numRows, truncate)
    }
    dataset
  }

  /**
   * 分配次执行指定的业务逻辑
   *
   * @param batch
   * 多大批次执行一次sinkFun中定义的操作
   * @param mapFun
   * 将Row类型映射为E类型的逻辑，并将处理后的数据放到listBuffer中
   * @param sinkFun
   * 具体处理逻辑，将数据sink到目标源
   */
  def foreachPartitionBatch[E](mapFun: T => E, sinkFun: ListBuffer[E] => Unit, batch: Int = 1000): Unit = {
    SparkUtils.datasetForeachPartitionBatch(this.dataset, mapFun, sinkFun, batch)
  }

  /**
   * spark datasource write api增强，提供配置文件进行覆盖配置
   *
   * @param format
   * DataSource中的format
   * @param saveMode
   * DataSource中的saveMode
   * @param saveParam
   * save方法的参数，可以是路径或表名：save(path)、saveAsTable(tableName)
   * @param isSaveTable
   * true：调用saveAsTable(saveParam)方法 false：调用save(saveParam)方法
   * @param options
   * DataSource中的options，支持参数传入和配置文件读取，相同的选项配置文件优先级更高
   * @param keyNum
   * 用于标识不同DataSource api所对应的配置文件中key的后缀
   */
  def writeEnhance(format: String = "",
                   saveMode: SaveMode = SaveMode.Append,
                   saveParam: String = "",
                   isSaveTable: Boolean = false,
                   options: Map[String, String] = Map.empty,
                   keyNum: Int = 1): Unit = {
    val finalFormat = if (noEmpty(FireSparkConf.datasourceFormat(keyNum))) FireSparkConf.datasourceFormat(keyNum) else format
    val finalSaveMode = if (noEmpty(FireSparkConf.datasourceSaveMode(keyNum))) SaveMode.valueOf(FireSparkConf.datasourceSaveMode(keyNum)) else saveMode
    val finalSaveParam = if (noEmpty(FireSparkConf.datasourceSaveParam(keyNum))) FireSparkConf.datasourceSaveParam(keyNum) else saveParam
    val finalIsSaveTable = if (noEmpty(FireSparkConf.datasourceIsSaveTable(keyNum))) FireSparkConf.datasourceIsSaveTable(keyNum).toBoolean else isSaveTable
    requireNonEmpty(dataset, finalFormat, finalSaveMode, finalSaveParam, finalIsSaveTable)

    this.logger.info(s"--> Spark DataSource write api参数信息（keyNum=$keyNum）<--")
    this.logger.info(s"format=${finalFormat} saveMode=${finalSaveMode} save参数=${finalSaveParam} saveToTable=${finalIsSaveTable}")

    val writer = dataset.write.format(finalFormat).options(SparkUtils.optionsEnhance(options, keyNum)).mode(finalSaveMode)
    if (!isSaveTable) {
      if (com.zto.fire.isEmpty(finalSaveMode)) writer.save() else writer.save(finalSaveParam)
    } else writer.saveAsTable(finalSaveParam)
  }

}