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

package com.zto.fire.spark.util

import com.zto.fire.common.conf.FireRocketMQConf
import com.zto.fire.common.util.{LogUtils, StringsUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.rocketmq.spark.{ConsumerStrategy, RocketMQConfig}
import org.slf4j.LoggerFactory

import com.zto.fire._

/**
 * RocketMQ相关工具类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-06-29 10:50
 */
object RocketMQUtils {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * rocketMQ配置信息
   *
   * @param groupId
   * 消费组
   * @return
   * rocketMQ相关配置
   */
  def rocketParams(rocketParam: JMap[String, String] = null,
                   groupId: String = null,
                   rocketNameServer: String = null,
                   tag: String = null,
                   keyNum: Int = 1): JMap[String, String] = {

    val optionParams = if (rocketParam != null) rocketParam else new JHashMap[String, String]()
    if (StringUtils.isNotBlank(groupId)) optionParams.put(RocketMQConfig.CONSUMER_GROUP, groupId)

    // rocket name server 配置
    val confNameServer = FireRocketMQConf.rocketNameServer(keyNum)
    val finalNameServer = if (StringUtils.isNotBlank(confNameServer)) confNameServer else rocketNameServer
    if (StringUtils.isNotBlank(finalNameServer)) optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, finalNameServer)

    // tag配置
    val confTag = FireRocketMQConf.rocketConsumerTag(keyNum)
    val finalTag = if (StringUtils.isNotBlank(confTag)) confTag else tag
    if (StringUtils.isNotBlank(finalTag)) optionParams.put(RocketMQConfig.CONSUMER_TAG, finalTag)

    // 每个分区拉取的消息数
    val maxSpeed = FireRocketMQConf.rocketPullMaxSpeedPerPartition(keyNum)
    if (StringUtils.isNotBlank(maxSpeed) && StringsUtils.isInt(maxSpeed)) optionParams.put(RocketMQConfig.MAX_PULL_SPEED_PER_PARTITION, maxSpeed)

    // 以spark.rocket.conf.开头的配置优先级最高
    val confMap = FireRocketMQConf.rocketConfMap(keyNum)
    if (confMap.nonEmpty) optionParams.putAll(confMap)
    // 日志记录RocketMQ的配置信息
    LogUtils.logMap(this.logger, optionParams.toMap, s"RocketMQ configuration. keyNum=$keyNum.")

    optionParams
  }

  /**
   * 根据消费位点字符串获取ConsumerStrategy实例
   * @param offset
   *               latest/earliest
   */
  def valueOfStrategy(offset: String): ConsumerStrategy = {
    if ("latest".equalsIgnoreCase(offset)) {
      ConsumerStrategy.lastest
    } else {
      ConsumerStrategy.earliest
    }
  }

}
