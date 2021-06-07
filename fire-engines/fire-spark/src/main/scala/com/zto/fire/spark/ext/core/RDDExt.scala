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
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.spark.connector.{HBaseBulkConnector, HBaseSparkBridge}
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}

/**
  * RDD相关扩展
  *
  * @author ChengLong 2019-5-18 10:28:31
  */
class RDDExt[T: ClassTag](rdd: RDD[T]) {
  private lazy val spark = SparkSingletonFactory.getSparkSession

  import spark.implicits._

  /**
    * 用于判断rdd是否为空
    *
    * @return
    * true: 不为空 false：为空
    */
  def isNotEmpty: Boolean = !rdd.isEmpty()

  /**
    * 遍历每个partition并打印元素到控制台
    */
  def printEachPartition: Unit = {
    rdd.foreachPartition(it => {
      it.foreach(item => println(item + " "))
    })
  }

  /**
    * 集群模式下打印数据
    */
  def printEachClusterPartition: Unit = {
    rdd.collect().foreach(println)
  }

  /**
    * 将rdd转为DataFrame
    */
  def toDF(): DataFrame = {
    this.spark.createDataFrame(rdd, classTag[T].runtimeClass)
  }

  /**
    * 将rdd转为DataFrame并注册成临时表
    *
    * @param tableName
    * 表名
    * @return
    * DataFrame
    */
  def createOrReplaceTempView(tableName: String, cache: Boolean = false): DataFrame = {
    val dataFrame = this.toDF()
    dataFrame.createOrReplaceTempView(tableName)
    if (cache) this.spark.cacheTables(tableName)
    dataFrame
  }

  /**
    * 根据RDD[String]批量删除
    *
    * @param tableName
    * HBase表名
    */
  def hbaseBulkDeleteRDD[T <: String : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.bulkDeleteRDD(tableName, rdd.asInstanceOf[RDD[String]], keyNum)
  }

  /**
    * 根据RDD[RowKey]批量删除记录
    *
    * @param tableName
    * rowKey集合
    */
  def hbaseDeleteRDD(tableName: String, keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbaseDeleteRDD(tableName, rdd.asInstanceOf[RDD[String]])
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
  def hbaseBulkGetRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], keyNum: Int = 1): RDD[E] = {
    HBaseBulkConnector.bulkGetRDD(tableName, rdd.asInstanceOf[RDD[String]], clazz, keyNum)
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
  def hbaseBulkGetDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], keyNum: Int = 1): DataFrame = {
    HBaseBulkConnector.bulkGetDF[E](tableName, rdd.asInstanceOf[RDD[String]], clazz, keyNum)
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
  def hbaseBulkGetDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], keyNum: Int = 1): Dataset[E] = {
    HBaseBulkConnector.bulkGetDS[E](tableName, rdd.asInstanceOf[RDD[String]], clazz, keyNum)
  }

  /**
    * 批量插入数据
    *
    * @param tableName
    * HBase表名
    * 数据集合，继承自HBaseBaseBean
    */
  def hbaseBulkPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.bulkPutRDD(tableName, rdd.asInstanceOf[RDD[T]], keyNum)
  }

  /**
    * 使用Spark API的方式将RDD中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    */
  def hbaseHadoopPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.hadoopPut(tableName, rdd.asInstanceOf[RDD[T]], keyNum)
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
  def hbaseGetRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], keyNum: Int = 1): RDD[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetRDD(tableName, clazz, rdd.asInstanceOf[RDD[String]])
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
  def hbaseGetDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], keyNum: Int = 1): Dataset[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetDS[T](tableName, clazz, rdd.asInstanceOf[RDD[String]])
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
  def hbaseGetDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], keyNum: Int = 1): DataFrame = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetDF(tableName, clazz, rdd.asInstanceOf[RDD[String]])
  }

  /**
    * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    */
  def hbasePutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutRDD[T](tableName, rdd.asInstanceOf[RDD[T]])
  }

  /**
    * 解析DStream中每个rdd的json数据，并转为DataFrame类型
    *
    * @param schema
    * 目标DataFrame类型的schema
    * @param isMySQL
    * 是否为mysql解析的消息
    * @param fieldNameUpper
    * 字段名称是否为大写
    * @param parseAll
    * 是否需要解析所有字段信息
    * @return
    */
  def kafkaJson2DFV(schema: Class[_], parseAll: Boolean = false, isMySQL: Boolean = true, fieldNameUpper: Boolean = false): DataFrame = {
    val ds = this.spark.createDataset(rdd.asInstanceOf[RDD[String]])(Encoders.STRING)
    val df = ds.select(from_json(new ColumnName("value"), SparkUtils.buildSchema2Kafka(schema, parseAll, isMySQL, fieldNameUpper)).as("data"))
    if (parseAll)
      df.select("data.*")
    else
      df.select("data.after.*")
  }

  /**
    * 解析DStream中每个rdd的json数据，并转为DataFrame类型
    *
    * @param schema
    * 目标DataFrame类型的schema
    * @param isMySQL
    * 是否为mysql解析的消息
    * @param fieldNameUpper
    * 字段名称是否为大写
    * @param parseAll
    * 是否解析所有字段信息
    * @return
    */
  def kafkaJson2DF(schema: Class[_], parseAll: Boolean = false, isMySQL: Boolean = true, fieldNameUpper: Boolean = false): DataFrame = {
    val ds = this.spark.createDataset(rdd.asInstanceOf[RDD[ConsumerRecord[String, String]]].map(t => t.value()))(Encoders.STRING)
    val structType = SparkUtils.buildSchema2Kafka(schema, parseAll, isMySQL, fieldNameUpper)
    val df = ds.select(from_json(new ColumnName("value"), structType).as("data"))
    val tmpDF = if (parseAll)
      df.select("data.*")
    else
      df.select("data.after.*")
    if (fieldNameUpper) tmpDF.toLowerDF else tmpDF
  }

  /**
    * 解析json数据，并注册为临时表
    *
    * @param tableName
    * 临时表名
    */
  def kafkaJson2Table(tableName: String, cacheTable: Boolean = false): Unit = {
    val msgDS = rdd.asInstanceOf[RDD[ConsumerRecord[String, String]]].map(t => t.value()).toDS()
    this.spark.read.json(msgDS).toLowerDF.createOrReplaceTempView(tableName)
    if (cacheTable) this.spark.cacheTables(tableName)
  }

  /**
    * 清空RDD的缓存
    */
  def uncache: Unit = {
    rdd.unpersist()
  }

  /**
    * 维护RocketMQ的offset
    */
  def kafkaCommitOffsets(stream: DStream[ConsumerRecord[String, String]]): Unit = {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }

  /**
    * 维护RocketMQ的offset
    */
  def rocketCommitOffsets(stream: InputDStream[MessageExt]): Unit = {
    val offsetRanges = rdd.asInstanceOf[org.apache.rocketmq.spark.HasOffsetRanges].offsetRanges
    stream.asInstanceOf[org.apache.rocketmq.spark.CanCommitOffsets].commitAsync(offsetRanges)
  }

  /**
   * 分配次执行指定的业务逻辑
   *
   * @param batch
   *            多大批次执行一次sinkFun中定义的操作
   * @param mapFun
   *            将Row类型映射为E类型的逻辑，并将处理后的数据放到listBuffer中
   * @param sinkFun
   * 具体处理逻辑，将数据sink到目标源
   */
  def foreachPartitionBatch[E](mapFun: T => E, sinkFun: ListBuffer[E] => Unit, batch: Int = 1000): Unit = {
    SparkUtils.rddForeachPartitionBatch(this.rdd, mapFun, sinkFun, batch)
  }
}