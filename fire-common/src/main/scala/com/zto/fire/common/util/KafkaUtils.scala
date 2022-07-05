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

package com.zto.fire.common.util

import com.zto.fire.common.conf.FireKafkaConf
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.util
import java.util.Properties

/**
 * Kafka工具类
 *
 * @author ChengLong 2020-4-17 09:50:50
 */
object KafkaUtils extends Logging {
  private lazy val kafkaMonitor = "fire_kafka_consumer"

  /**
   * 根据kafka集群名称获取broker地址
   *
   * @param clusterName 集群名称
   * @return broker地址
   */
  def getBorkers(clusterName: String): String = FireKafkaConf.kafkaMap.getOrElse(clusterName, "")

  /**
   * 创建新的kafka consumer
   *
   * @param host    kafka broker地址
   * @param groupId 对应的groupId
   * @return KafkaConsumer
   */
  def createNewConsumer(host: String, groupId: String): KafkaConsumer[String, String] = {
    val properties = new Properties
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put("auto.offset.reset", "earliest")
    new KafkaConsumer[String, String](properties)
  }

  /**
   * 获取大于指定时间戳的一条消息
   *
   * @param host      broker地址
   * @param topic     topic信息
   * @param timestamp 消息时间戳
   * @return 一条消息记录
   */
  def getMsg(host: String, topic: String, timestamp: java.lang.Long): String = {
    var kafkaConsumer: KafkaConsumer[String, String] = null
    var msg = ""
    try {
      kafkaConsumer = createNewConsumer(host, kafkaMonitor)
      // 如果指定了时间戳，则取大于该时间戳的消息
      if (timestamp != null) { // 获取topic的partition信息
        val partitionInfos = kafkaConsumer.partitionsFor(topic)
        val topicPartitions = new util.ArrayList[TopicPartition]
        val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]
        for (partitionInfo <- partitionInfos) {
          topicPartitions.add(new TopicPartition(partitionInfo.topic, partitionInfo.partition))
          timestampsToSearch.put(new TopicPartition(partitionInfo.topic, partitionInfo.partition), timestamp)
        }
        // 手动指定各分区offset
        kafkaConsumer.assign(topicPartitions)
        // 获取每个partition指定时间戳的偏移量
        val map = kafkaConsumer.offsetsForTimes(timestampsToSearch)
        this.logger.info("根据时间戳获取偏移量：map.size={}", map.size())
        var offsetTimestamp: OffsetAndTimestamp = null
        this.logger.info("开始设置各分区初始偏移量...")
        for (entry <- map.entrySet) { // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
          offsetTimestamp = entry.getValue
          if (offsetTimestamp != null) { // 设置读取消息的偏移量
            val offset: java.lang.Long = offsetTimestamp.offset
            kafkaConsumer.seek(entry.getKey, offset)
            this.logger.info("seek: id=" + entry.getKey.partition + " offset=" + offset)
          }
        }
      } else { // 如果未指定时间戳，则直接获取消息
        kafkaConsumer.subscribe(util.Arrays.asList(topic))
      }
      // 消费消息
      val records = kafkaConsumer.poll(10000)
      for (record <- records if StringUtils.isBlank(msg)) {
        if (timestamp == null) {
          msg = record.value
        }
        else { // 如果指定时间戳，则取大于指定时间戳的消息
          if (record.timestamp >= timestamp) {
            msg = record.value
          }
        }
      }
    } catch {
      case e: Exception => logger.error("获取消息失败", e)
    } finally {
      if (kafkaConsumer != null) kafkaConsumer.close()
    }
    msg
  }

  /**
   * kafka配置信息
   *
   * @param kafkaParams
   * 代码中指定的kafka配置信息，如果配置文件中也有配置，则配置文件中的优先级高
   * @param groupId
   * 消费组
   * @param offset
   * smallest、largest
   * @return
   * kafka相关配置
   */
  def kafkaParams(kafkaParams: Map[String, Object] = null,
                  groupId: String = null,
                  kafkaBrokers: String = null,
                  offset: String = FireKafkaConf.offsetLargest,
                  autoCommit: Boolean = false,
                  keyNum: Int = 1): Map[String, Object] = {

    val consumerMap = collection.mutable.Map[String, Object]()
    // 代码中指定的kafka配置优先级最低
    if (kafkaParams != null && kafkaParams.nonEmpty) consumerMap ++= kafkaParams

    // 如果没有在配置文件中指定brokers，则认为从代码中获取，此处返回空的map，用于上层判断
    val confBrokers = FireKafkaConf.kafkaBrokers(keyNum)
    val finalKafkaBrokers = if (StringUtils.isNotBlank(confBrokers)) confBrokers else kafkaBrokers
    if (StringUtils.isNotBlank(finalKafkaBrokers)) consumerMap += ("bootstrap.servers" -> finalKafkaBrokers)

    // 如果配置文件中没有指定spark.kafka.group.id，则默认获取用户指定的groupId
    val confGroupId = FireKafkaConf.kafkaGroupId(keyNum)
    val finalKafkaGroupId = if (StringUtils.isNotBlank(confGroupId)) confGroupId else groupId
    if (StringUtils.isNotBlank(finalKafkaGroupId)) consumerMap += ("group.id" -> finalKafkaGroupId)

    val confOffset = FireKafkaConf.kafkaStartingOffset(keyNum)
    val finalOffset = if (StringUtils.isNotBlank(confOffset)) confOffset else offset
    if (StringUtils.isNotBlank(finalOffset)) consumerMap += ("auto.offset.reset" -> finalOffset)

    val confAutoCommit = FireKafkaConf.kafkaEnableAutoCommit(keyNum)
    val finalAutoCommit = if (confAutoCommit != null) confAutoCommit else autoCommit
    if (finalAutoCommit != null) consumerMap += ("enable.auto.commit" -> (finalAutoCommit: java.lang.Boolean))

    // 最基本的配置项
    consumerMap ++= collection.mutable.Map[String, Object](
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "session.timeout.ms" -> FireKafkaConf.kafkaSessionTimeOut(keyNum),
      "request.timeout.ms" -> FireKafkaConf.kafkaRequestTimeOut(keyNum),
      "max.poll.interval.ms" -> FireKafkaConf.kafkaPollInterval(keyNum)
    )

    // 以spark.kafka.conf.开头的配置优先级最高
    val configMap = FireKafkaConf.kafkaConfMapWithType(keyNum)
    if (configMap.nonEmpty) consumerMap ++= configMap
    // 日志记录最终生效的kafka配置
    LogUtils.logMap(this.logger, consumerMap.toMap, s"Kafka client configuration. keyNum=$keyNum.")

    consumerMap.toMap
  }

}
