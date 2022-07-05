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
import com.zto.fire.common.util.{ExceptionBus, Logging}
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.spark.connector.HBaseBulkConnector
import com.zto.fire.spark.util.SparkSingletonFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{CanCommitOffsets => RocketCanCommitOffsets}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import scala.reflect._
import scala.util.Try

/**
 * DStream扩展
 *
 * @param stream
 * stream对象
 * @author ChengLong 2019-5-18 11:06:56
 */
class DStreamExt[T: ClassTag](stream: DStream[T]) extends Logging {
  private[this] lazy val spark = SparkSingletonFactory.getSparkSession

  /**
   * DStrea数据实时写入
   *
   * @param tableName
   * HBase表名
   */
  def hbaseBulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, keyNum: Int = 1): Unit = {
    HBaseBulkConnector.bulkPutStream(tableName, stream.asInstanceOf[DStream[T]], keyNum)
  }

  /**
   * 清空RDD的缓存
   */
  def uncache: Unit = {
    stream.persist(StorageLevel.NONE)
  }

  /**
   * 维护kafka的offset，生产下可能会导致丢数据的风险
   * use rdd.kafkaCommitOffsets(dStream)
   */
  @deprecated("rdd.kafkaCommitOffsets", since = "2.2.0")
  def kafkaCommitOffsets[T <: ConsumerRecord[String, String]]: Unit = {
    stream.asInstanceOf[DStream[T]].foreachRDD { rdd =>
      try {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /**
   * 维护RocketMQ的offset，生产下可能会导致丢数据的风险
   * use rdd.rocketCommitOffsets(dStream)
   */
  @deprecated("rdd.rocketCommitOffsets", since = "2.2.0")
  def rocketCommitOffsets[T <: MessageExt]: Unit = {
    stream.asInstanceOf[DStream[T]].foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        try {
          val offsetRanges = rdd.asInstanceOf[org.apache.rocketmq.spark.HasOffsetRanges].offsetRanges
          stream.asInstanceOf[org.apache.rocketmq.spark.CanCommitOffsets].commitAsync(offsetRanges)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  /**
   * 至少一次的语义保证，当rdd处理成功时提交offset，当处理失败时重试指定的次数
   * 该算子支持识别kafka和rocketmq的源，并在执行成功的情况下提交offset
   * 注：必须在最原始的DStream上调用该算子，不能经过任何的transform转换，否则会报错
   *
   * @param process
   * rdd的处理逻辑
   * @param reTry
   * rdd处理失败重试的次数
   * @param exitOnFailure
   * 当重试多次仍失败时是否退出
   */
  def foreachRDDAtLeastOnce(process: RDD[T] => Unit)(implicit reTry: Int = 3, duration: Long = 3000, autoCommit: Boolean = true, exitOnFailure: Boolean = true): Unit = {
    this.stream.foreachRDD((rdd, batchTime) => {
      // 用户的业务逻辑处理，对于处理失败的RDD重试指定的次数
      val retValue = Try {
        try {
          retry(reTry, duration) {
            process(rdd)
          }
        }
      }

      // 根据rdd处理的成功与否决定是否提交offset或退出任务
      if (retValue.isSuccess) {
        if (autoCommit) {
          this.stream match {
            // 提交kafka的offset
            case dstream: CanCommitOffsets => {
              rdd.kafkaCommitOffsets(dstream.asInstanceOf[DStream[ConsumerRecord[String, String]]])
              this.logger.info(s"批次[${batchTime}]执行成功，kafka offset提交成功")
            }
            // 提交rocketmq的offset
            case dstream: RocketCanCommitOffsets => {
              rdd.rocketCommitOffsets(dstream.asInstanceOf[InputDStream[MessageExt]])
              this.logger.info(s"批次[${batchTime}]执行成功，rocketmq offset提交成功")
            }
            case _ => throw new IllegalArgumentException("DStream必须为最原始的source流，不能经过transformation算子做转换！")
          }
        }
      } else if (exitOnFailure) {
        this.logger.error(s"批次[${batchTime}]执行失败，offset未提交，任务将退出")
        this.logger.error(s"异常堆栈：${ExceptionBus.stackTrace(retValue.failed.get)}")
        SparkSingletonFactory.getStreamingContext.stop(true, false)
      }
    })
  }
}