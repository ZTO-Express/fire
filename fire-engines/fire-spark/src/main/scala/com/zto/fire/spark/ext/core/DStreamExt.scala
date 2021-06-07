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

import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.spark.connector.HBaseBulkConnector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import scala.reflect._

/**
 * DStream扩展
 *
 * @param stream
 * stream对象
 * @author ChengLong 2019-5-18 11:06:56
 */
class DStreamExt[T: ClassTag](stream: DStream[T]) {

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
   * 维护kafka的offset
   */
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
   * 维护RocketMQ的offset
   */
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
}