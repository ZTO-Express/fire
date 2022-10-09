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
import com.zto.fire.common.enu.{Operation => FOperation}
import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf}
import com.zto.fire.common.util.{LineageManager, Logging}
import com.zto.fire.spark.util.{RocketMQUtils, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy, RocketMQConfig, RocketMqUtils}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils

/**
 * StreamingContext扩展
 *
 * @param ssc
 * StreamingContext对象
 * @author ChengLong 2019-5-18 11:03:59
 */
class StreamingContextExt(ssc: StreamingContext) extends Logging {

  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

  private[this] lazy val appName = ssc.sparkContext.appName

  /**
   * 创建DStream流
   *
   * @param kafkaParams
   * kafka参数
   * @param topics
   * topic列表
   * @return
   * DStream
   */
  def createDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, groupId: String = null, keyNum: Int = 1): DStream[ConsumerRecord[String, String]] = {
    // kafka topic优先级：配置文件 > topics参数
    val confTopic = FireKafkaConf.kafkaTopics(keyNum)
    val finalKafkaTopic = if (StringUtils.isNotBlank(confTopic)) SparkUtils.topicSplit(confTopic) else topics
    require(finalKafkaTopic != null && finalKafkaTopic.nonEmpty, s"kafka topic不能为空，请在配置文件中指定：spark.kafka.topics$keyNum")
    this.logger.info(s"kafka topic is $finalKafkaTopic")

    val confKafkaParams = com.zto.fire.common.util.KafkaUtils.kafkaParams(kafkaParams, groupId, keyNum = keyNum)
    require(confKafkaParams.nonEmpty, "kafka相关配置不能为空！")
    require(confKafkaParams.contains("bootstrap.servers"), s"kafka bootstrap.servers不能为空，请在配置文件中指定：spark.kafka.brokers.name$keyNum")
    require(confKafkaParams.contains("group.id"), s"kafka group.id不能为空，请在配置文件中指定：spark.kafka.group.id$keyNum")

    // kafka消费信息埋点
    LineageManager.addMQDatasource("kafka", confKafkaParams("bootstrap.servers").toString, finalKafkaTopic.mkString("", ", ", ""), confKafkaParams("group.id").toString, FOperation.SOURCE)

    KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](finalKafkaTopic, confKafkaParams))
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
  def createRocketPullStream(rocketParam: JMap[String, String] = null,
                             groupId: String = this.appName,
                             topics: String = null,
                             tag: String = null,
                             consumerStrategy: ConsumerStrategy = ConsumerStrategy.lastest,
                             locationStrategy: LocationStrategy = LocationStrategy.PreferConsistent,
                             instance: String = "",
                             keyNum: Int = 1): DStream[MessageExt] = {

    // 获取topic信息，配置文件优先级高于代码中指定的
    val confTopics = FireRocketMQConf.rocketTopics(keyNum)
    val finalTopics = if (StringUtils.isNotBlank(confTopics)) confTopics else topics
    require(StringUtils.isNotBlank(finalTopics), s"RocketMQ的Topics不能为空，请在配置文件中指定：spark.rocket.topics$keyNum")

    // 起始消费位点
    val confOffset = FireRocketMQConf.rocketStartingOffset(keyNum)
    val finalConsumerStrategy = if (StringUtils.isNotBlank(confOffset)) RocketMQUtils.valueOfStrategy(confOffset) else consumerStrategy

    // 是否自动提交offset
    val finalAutoCommit = FireRocketMQConf.rocketEnableAutoCommit(keyNum)

    // groupId信息
    val confGroupId = FireRocketMQConf.rocketGroupId(keyNum)
    val finalGroupId = if (StringUtils.isNotBlank(confGroupId)) confGroupId else groupId
    require(StringUtils.isNotBlank(finalGroupId), s"RocketMQ的groupId不能为空，请在配置文件中指定：spark.rocket.group.id$keyNum")

    // 详细的RocketMQ配置信息
    val finalRocketParam = RocketMQUtils.rocketParams(rocketParam, finalGroupId, rocketNameServer = null, tag = tag, keyNum)
    require(!finalRocketParam.isEmpty, "RocketMQ相关配置不能为空！")
    require(finalRocketParam.containsKey(RocketMQConfig.NAME_SERVER_ADDR), s"RocketMQ nameserver.addr不能为空，请在配置文件中指定：spark.rocket.brokers.name$keyNum")
    require(finalRocketParam.containsKey(RocketMQConfig.CONSUMER_TAG), s"RocketMQ tag不能为空，请在配置文件中指定：spark.rocket.consumer.tag$keyNum")
    // 消费者标识
    val instanceId = FireRocketMQConf.rocketInstanceId(keyNum)
    val finalInstanceId = if (StringUtils.isNotBlank(instanceId)) instanceId else instance
    if (StringUtils.isNotBlank(finalInstanceId)) finalRocketParam.put("consumer.instance", finalInstanceId)

    // 消费rocketmq埋点信息
    LineageManager.addMQDatasource("rocketmq", finalRocketParam(RocketMQConfig.NAME_SERVER_ADDR), finalTopics, finalGroupId, FOperation.SOURCE)

    val inputDStream = RocketMqUtils.createMQPullStream(this.ssc,
      finalGroupId,
      finalTopics.split(",").toList,
      finalConsumerStrategy,
      finalAutoCommit,
      forceSpecial = FireRocketMQConf.rocketForceSpecial(keyNum),
      failOnDataLoss = FireRocketMQConf.rocketFailOnDataLoss(keyNum),
      locationStrategy, finalRocketParam)
      if ("*".equals(finalRocketParam(RocketMQConfig.CONSUMER_TAG))) {
        inputDStream
      } else {
        inputDStream.filter(msg => msg.getTags.equals(finalRocketParam(RocketMQConfig.CONSUMER_TAG)))
      }
  }

  /**
   * 开启streaming
   */
  def startAwaitTermination(): Unit = {
    ssc.start()
    ssc.awaitTermination()
    Thread.currentThread().join()
  }

  /**
   * 提交Spark Streaming Graph并执行
   */
  def start: Unit = this.startAwaitTermination()
}