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

import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils

/**
 * RocketMQ相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:58
 */
private[fire] object FireRocketMQConf {
  lazy val rocketOffsetLargest = "latest"
  lazy val rocketOffsetSmallest = "earliest"
  lazy val rocketConsumerTag = "*"
  lazy val rocketClusterMapConfStart = "rocket.cluster.map."
  // 初始化kafka集群名称与地址映射
  private[fire] lazy val rocketClusterMap = PropUtils.sliceKeys(rocketClusterMapConfStart)
  lazy val rocketConfStart = "rocket.conf."
  // rocketMQ name server
  lazy val ROCKET_BROKERS_NAME = "rocket.brokers.name"
  // rocketMQ topic信息，多个以逗号分隔
  lazy val ROCKET_TOPICS = "rocket.topics"
  // rocketMQ groupId
  val ROCKET_GROUP_ID = "rocket.group.id"
  // 丢失数据是否失败
  lazy val ROCKET_FAIL_ON_DATA_LOSS = "rocket.failOnDataLoss"
  lazy val ROCKET_FORCE_SPECIAL = "rocket.forceSpecial"
  // 是否自动维护offset
  lazy val ROCKET_ENABLE_AUTO_COMMIT = "rocket.enable.auto.commit"
  // RocketMQ起始消费位点
  lazy val ROCKET_STARTING_OFFSET = "rocket.starting.offsets"
  // rocketMq订阅的tag
  lazy val ROCKET_CONSUMER_TAG = "rocket.consumer.tag"
  // 每次拉取每个partition的消息数
  lazy val ROCKET_PULL_MAX_SPEED_PER_PARTITION = "rocket.pull.max.speed.per.partition"
  lazy val ROCKET_INSTANCE_ID = "rocket.consumer.instance"

  // 用于标识消费者的名称
  def rocketInstanceId(keyNum: Int = 1): String = PropUtils.getString(this.ROCKET_INSTANCE_ID, "", keyNum)
  // rocket-client配置信息
  def rocketConfMap(keyNum: Int = 1): collection.immutable.Map[String, String] = PropUtils.sliceKeysByNum(rocketConfStart, keyNum)
  // 获取消费位点
  def rocketStartingOffset(keyNum: Int = 1): String = PropUtils.getString(this.ROCKET_STARTING_OFFSET, "", keyNum)
  // 丢失数据时是否失败
  def rocketFailOnDataLoss(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.ROCKET_FAIL_ON_DATA_LOSS, true, keyNum)
  // spark.rocket.forceSpecial
  def rocketForceSpecial(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.ROCKET_FORCE_SPECIAL, false, keyNum)
  // enable.auto.commit
  def rocketEnableAutoCommit(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.ROCKET_ENABLE_AUTO_COMMIT, false, keyNum)
  // 获取rocketMQ 订阅的tag
  def rocketConsumerTag(keyNum: Int = 1): String = PropUtils.getString(this.ROCKET_CONSUMER_TAG, "", keyNum)
  // 获取groupId
  def rocketGroupId(keyNum: Int = 1): String = PropUtils.getString(this.ROCKET_GROUP_ID, "", keyNum)
  // 获取rocket topic列表
  def rocketTopics(keyNum: Int = 1): String = PropUtils.getString(this.ROCKET_TOPICS, null, keyNum)
  // 每次拉取每个partition的消息数
  def rocketPullMaxSpeedPerPartition(keyNum: Int = 1): String = PropUtils.getString(this.ROCKET_PULL_MAX_SPEED_PER_PARTITION, "", keyNum)

  // 获取rocketMQ name server 地址
  def rocketNameServer(keyNum: Int = 1): String = {
    val brokerName = PropUtils.getString(this.ROCKET_BROKERS_NAME, "", keyNum)
    this.rocketClusterMap.getOrElse(brokerName, brokerName)
  }
}