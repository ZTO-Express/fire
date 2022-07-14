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
import com.zto.fire.core.Api
import com.zto.fire.jdbc.JdbcConnectorBridge
import com.zto.fire.spark.bean.GenerateBean
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.connector.{BeanGenReceiver, DataGenReceiver}
import com.zto.fire.spark.ext.provider._
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

import java.io.InputStream
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * SparkContext扩展
 *
 * @param spark
 * sparkSession对象
 * @author ChengLong 2019-5-18 10:51:19
 */
class SparkSessionExt(spark: SparkSession) extends Api with JdbcConnectorBridge with JdbcSparkProvider
  with HBaseBulkProvider with SqlProvider with HBaseConnectorProvider with HBaseHadoopProvider with KafkaSparkProvider {
  private[fire] lazy val ssc = SparkSingletonFactory.getStreamingContext
  private[this] lazy val appName = ssc.sparkContext.appName

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.sc.parallelize(seq, numSlices)
  }

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def createRDD[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.parallelize[T](seq, numSlices)
  }

  /**
   * 创建socket流
   */
  def createSocketStream[T: ClassTag](
                                       hostname: String,
                                       port: Int,
                                       converter: (InputStream) => Iterator[T],
                                       storageLevel: StorageLevel
                                     ): ReceiverInputDStream[T] = {
    this.ssc.socketStream[T](hostname, port, converter, storageLevel)
  }

  /**
   * 创建socket文本流
   */
  def createSocketTextStream(
                              hostname: String,
                              port: Int,
                              storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                            ): ReceiverInputDStream[String] = {
    this.ssc.socketTextStream(hostname, port, storageLevel)
  }


  /**
   * 构建Kafka DStream流
   *
   * @param kafkaParams
   * kafka参数
   * @param topics
   * topic列表
   * @return
   * DStream
   */
  def createKafkaDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, groupId: String = null, keyNum: Int = 1): DStream[ConsumerRecord[String, String]] = {
    this.ssc.createDirectStream(kafkaParams, topics, groupId, keyNum)
  }

  /**
   * 构建RocketMQ拉取消息的DStream流
   *
   * @param rocketParam
   * rocketMQ相关消费参数
   * @param groupId
   * groupId
   * @param topics
   * topic列表
   * @param consumerStrategy
   * 从何处开始消费
   * @return
   * rocketMQ DStream
   */
  def createRocketMqPullStream(rocketParam: JMap[String, String] = null,
                               groupId: String = this.appName,
                               topics: String = null,
                               tag: String = null,
                               consumerStrategy: ConsumerStrategy = ConsumerStrategy.lastest,
                               locationStrategy: LocationStrategy = LocationStrategy.PreferConsistent,
                               instance: String = "",
                               keyNum: Int = 1): DStream[MessageExt] = {
    this.ssc.createRocketPullStream(rocketParam, groupId, topics, tag, consumerStrategy, locationStrategy, instance, keyNum)
  }

  /**
   * 创建根据指定规则生成对象实例的DataGenReceiver
   *
   * @param delay
   * 数据生成间隔时间（ms）
   * @param generateFun
   * 数据生成规则
   * @tparam T
   * 生成数据的类型
   * @return
   * ReceiverInputDStream[T]
   */
  def createDataGenStream[T <: GenerateBean[T] : ClassTag](delay: Long = 1000, generateFun: => mutable.Buffer[T]): ReceiverInputDStream[T] = {
    this.receiverStream[T](new DataGenReceiver[T](delay, generateFun = generateFun))
  }

  /**
   * 创建根据指定规则生成对象实例的BeanDataGenReceiver
   *
   * @param delay
   * 数据生成间隔时间（ms）
   * @tparam T
   * 生成数据的类型
   * @return
   * ReceiverInputDStream[T]
   */
  def createBeanGenStream[T <: GenerateBean[T] : ClassTag](delay: Long = 1000): ReceiverInputDStream[T] = {
    this.receiverStream[T](new BeanGenReceiver[T](delay))
  }

  /**
   * 接受自定义receiver的数据
   *
   * @param receiver
   * 自定义receiver
   * @tparam T
   * 接受的数据类型
   * @return
   * 包装后的DStream[T]
   */
  def receiverStream[T: ClassTag](receiver: Receiver[T]): ReceiverInputDStream[T] = {
    this.ssc.receiverStream[T](receiver)
  }

  /**
   * 启动StreamingContext
   */
  override def start(): Unit = {
    if (this.ssc != null) {
      this.ssc.startAwaitTermination()
    }
  }

  /**
   * spark datasource read api增强，提供配置文件进行覆盖配置
   *
   * @param format
   * DataSource中的format
   * @param loadParams
   * load方法的参数，多个路径以逗号分隔
   * @param options
   * DataSource中的options，支持参数传入和配置文件读取，相同的选项配置文件优先级更高
   * @param keyNum
   * 用于标识不同DataSource api所对应的配置文件中key的后缀
   */
  def readEnhance(format: String = "",
                  loadParams: Seq[String] = null,
                  options: Map[String, String] = Map.empty,
                  keyNum: Int = 1): Unit = {
    val finalFormat = if (noEmpty(FireSparkConf.datasourceFormat(keyNum))) FireSparkConf.datasourceFormat(keyNum) else format
    val finalLoadParam = if (noEmpty(FireSparkConf.datasourceLoadParam(keyNum))) FireSparkConf.datasourceLoadParam(keyNum).split(",").toSeq else loadParams
    this.logger.info(s"--> Spark DataSource read api参数信息（keyNum=$keyNum）<--")
    this.logger.info(s"format=${finalFormat} loadParams=${finalLoadParam}")

    requireNonEmpty(finalFormat, finalLoadParam)
    SparkSingletonFactory.getSparkSession.read.format(format).options(SparkUtils.optionsEnhance(options, keyNum)).load(finalLoadParam: _*)
  }
}