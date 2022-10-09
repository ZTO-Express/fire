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

package com.zto.fire.common.conf

import com.zto.fire.common.util.{PropUtils, StringsUtils}

/**
 * kafka相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:58
 */
private[fire] object FireKafkaConf {
  lazy val offsetLargest = "latest"
  lazy val offsetSmallest = "earliest"
  lazy val offsetNone = "none"
  lazy val clusterMapConfStart = "fire.kafka.cluster.map."
  lazy val kafkaConfStart = "kafka.conf."
  lazy val KAFKA_BROKERS_NAME = "kafka.brokers.name"
  // kafka的topic列表，以逗号分隔
  lazy val KAFKA_TOPICS = "kafka.topics"
  // group.id
  lazy val KAFKA_GROUP_ID = "kafka.group.id"
  // kafka起始消费位点
  lazy val KAFKA_STARTING_OFFSET = "kafka.starting.offsets"
  // kafka结束消费位点
  lazy val KAFKA_ENDING_OFFSET = "kafka.ending.offsets"
  // 是否自动维护offset
  lazy val KAFKA_ENABLE_AUTO_COMMIT = "kafka.enable.auto.commit"
  // 丢失数据是否失败
  lazy val KAFKA_FAIL_ON_DATA_LOSS = "kafka.failOnDataLoss"
  // kafka session超时时间
  lazy val KAFKA_SESSION_TIMEOUT_MS = "kafka.session.timeout.ms"
  // kafka request超时时间
  lazy val KAFKA_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms"
  lazy val KAFKA_MAX_POLL_INTERVAL_MS = "kafka.max.poll.interval.ms"
  lazy val KAFKA_COMMIT_OFFSETS_ON_CHECKPOINTS = "kafka.CommitOffsetsOnCheckpoints"
  lazy val KAFKA_START_FROM_TIMESTAMP = "kafka.StartFromTimestamp"
  lazy val KAFKA_START_FROM_GROUP_OFFSETS = "kafka.StartFromGroupOffsets"

  // 是否使状态中存放的offset不生效（请谨慎配置，用于kafka集群迁移等不正常状况的运维）
  lazy val KAFKA_OVERWRITE_STATE_OFFSET = "kafka.force.overwrite.stateOffset.enable"
  // 是否在开启checkpoint的情况下强制开启周期性offset提交
  lazy val KAFKA_FORCE_AUTO_COMMIT = "kafka.force.autoCommit.enable"
  // 周期性提交offset的时间间隔（ms）
  lazy val KAFKA_FORCE_AUTO_COMMIT_INTERVAL = "kafka.force.autoCommit.Interval"

  // 初始化kafka集群名称与地址映射
  private[fire] lazy val kafkaMap = PropUtils.sliceKeys(clusterMapConfStart)

  // kafka消费起始位点
  def kafkaStartingOffset(keyNum: Int = 1): String = PropUtils.getString(this.KAFKA_STARTING_OFFSET, "", keyNum)

  // kafka消费结束位点
  def kafkaEndingOffsets(keyNum: Int = 1): String = PropUtils.getString(this.KAFKA_ENDING_OFFSET, "", keyNum)

  // 丢失数据时是否失败
  def kafkaFailOnDataLoss(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.KAFKA_FAIL_ON_DATA_LOSS, true, keyNum)

  // enable.auto.commit
  def kafkaEnableAutoCommit(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.KAFKA_ENABLE_AUTO_COMMIT, false, keyNum)

  // 获取topic列表
  def kafkaTopics(keyNum: Int = 1): String = PropUtils.getString(this.KAFKA_TOPICS, "", keyNum)

  // kafka session超时时间，默认5分钟
  def kafkaSessionTimeOut(keyNum: Int = 1): java.lang.Integer = PropUtils.getInt(this.KAFKA_SESSION_TIMEOUT_MS, 300000, keyNum)

  // kafka request超时时间，默认10分钟
  def kafkaPollInterval(keyNum: Int = 1): java.lang.Integer = PropUtils.getInt(this.KAFKA_MAX_POLL_INTERVAL_MS, 600000, keyNum)

  // kafka request超时时间
  def kafkaRequestTimeOut(keyNum: Int = 1): java.lang.Integer = PropUtils.getInt(this.KAFKA_REQUEST_TIMEOUT_MS, 400000, keyNum)

  // 配置文件中的groupId
  def kafkaGroupId(keyNum: Int = 1): String = PropUtils.getString(this.KAFKA_GROUP_ID, "", keyNum)

  // 是否在checkpoint时记录offset值
  def kafkaCommitOnCheckpoint(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.KAFKA_COMMIT_OFFSETS_ON_CHECKPOINTS, true, keyNum)

  // 设置从指定时间戳位置开始消费kafka
  def kafkaStartFromTimeStamp(keyNum: Int = 1): java.lang.Long = PropUtils.getLong(this.KAFKA_START_FROM_TIMESTAMP, 0L, keyNum)

  // 从topic中指定的group上次消费的位置开始消费，必须配置group.id参数
  def kafkaStartFromGroupOffsets(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.KAFKA_START_FROM_GROUP_OFFSETS, false, keyNum)

  // kafka-client配置信息
  def kafkaConfMap(keyNum: Int = 1): collection.immutable.Map[String, String] = PropUtils.sliceKeysByNum(kafkaConfStart, keyNum)

  // 是否使状态中存放的offset不生效
  def kafkaForceOverwriteStateOffset: Boolean = PropUtils.getBoolean(this.KAFKA_OVERWRITE_STATE_OFFSET, false)
  // 是否在开启checkpoint的情况下强制开启周期性offset提交
  def kafkaForceCommit: Boolean = PropUtils.getBoolean(this.KAFKA_FORCE_AUTO_COMMIT, false)
  // 周期性提交offset的时间间隔（ms）
  def kafkaForceCommitInterval: Long = PropUtils.getLong(this.KAFKA_FORCE_AUTO_COMMIT_INTERVAL, 30000)

  def kafkaConfMapWithType(keyNum: Int = 1): collection.immutable.Map[String, Object] = {
    val map = new collection.mutable.HashMap[String, Object]()
    this.kafkaConfMap(keyNum).foreach(kv => {
      map.put(kv._1, StringsUtils.parseString(kv._2))
    })
    map.toMap
  }

  /**
   * 根据名称获取kafka broker地址
   */
  def kafkaBrokers(keyNum: Int = 1): String = {
    val brokerName = PropUtils.getString(this.KAFKA_BROKERS_NAME, "", keyNum)
    this.kafkaBrokers(brokerName)
  }

  /**
   * 根据url或别名返回真实的url地址
   */
  def kafkaBrokers(url: String): String = {
    this.kafkaMap.getOrElse(url, url)
  }
}