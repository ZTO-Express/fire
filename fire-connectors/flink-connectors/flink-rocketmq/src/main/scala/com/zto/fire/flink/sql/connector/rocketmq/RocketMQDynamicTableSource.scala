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

package com.zto.fire.flink.sql.connector.rocketmq

import com.zto.fire.common.conf.FireRocketMQConf
import com.zto.fire.common.util.LineageManager
import com.zto.fire.flink.sql.connector.rocketmq.RocketMQOptions.getRocketMQProperties
import com.zto.fire.predef._
import com.zto.fire.common.enu.{Operation => FOperation}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.DataTypeUtils
import org.apache.rocketmq.flink.common.serialization.JsonDeserializationSchema
import org.apache.rocketmq.flink.{RocketMQConfig, RocketMQSourceWithTag}

/**
 * 定义source table
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class RocketMQDynamicTableSource(physicalDataType: DataType,
                                 keyDecodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                                 valueDecodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                                 keyProjection: Array[Int],
                                 valueProjection: Array[Int],
                                 keyPrefix: String,
                                 tableOptions: JMap[String, String]) extends ScanTableSource {

  override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()

  override def copy(): DynamicTableSource = new RocketMQDynamicTableSource(physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, tableOptions)

  override def asSummaryString(): String = "fire-rocketmq source"

  /**
   * 创建反序列化器
   */
  def createDeserialization(context: DynamicTableSource.Context, format: DecodingFormat[DeserializationSchema[RowData]], projection: Array[Int], prefix: String): DeserializationSchema[RowData] = {
    if (format == null) return null

    var physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection)
    if (noEmpty(prefix)) {
      physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix)
    }
    format.createRuntimeDecoder(context, physicalFormatDataType)
  }

  /**
   * 消费rocketmq中的数据，并反序列化为RowData对象实例
   */
  override def getScanRuntimeProvider(context: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    // 获取以rocket.conf.为前缀的配置
    val properties = getRocketMQProperties(this.tableOptions)

    // 获取rocket.brokers.name对应的nameserver地址
    val brokerName = tableOptions.get(FireRocketMQConf.ROCKET_BROKERS_NAME)
    val nameserver = FireRocketMQConf.rocketClusterMap.getOrElse(brokerName, brokerName)
    if (noEmpty(nameserver)) properties.setProperty(RocketMQConfig.NAME_SERVER_ADDR, nameserver)
    assert(noEmpty(properties.getProperty(RocketMQConfig.NAME_SERVER_ADDR)), s"""nameserver不能为空，请在with中使用 '${FireRocketMQConf.ROCKET_BROKERS_NAME}'='ip:port' 指定""")

    // 获取topic信息
    val topic = tableOptions.get(FireRocketMQConf.ROCKET_TOPICS)
    if (noEmpty(topic)) properties.setProperty(RocketMQConfig.CONSUMER_TOPIC, topic)
    assert(noEmpty(properties.getProperty(RocketMQConfig.CONSUMER_TOPIC)), s"""topic不能为空，请在with中使用 '${FireRocketMQConf.ROCKET_TOPICS}'='topicName' 指定""")

    // 获取groupId信息
    val groupId = tableOptions.get(FireRocketMQConf.ROCKET_GROUP_ID)
    if (noEmpty(groupId)) properties.setProperty(RocketMQConfig.CONSUMER_GROUP, groupId)
    assert(noEmpty(properties.getProperty(RocketMQConfig.CONSUMER_GROUP)), s"""group.id不能为空，请在with中使用 '${FireRocketMQConf.ROCKET_GROUP_ID}'='groupId' 指定""")

    // 获取tag信息
    val tag = tableOptions.get(FireRocketMQConf.ROCKET_CONSUMER_TAG)
    if (noEmpty(tag)) properties.setProperty(RocketMQConfig.CONSUMER_TAG, tag) else properties.setProperty(RocketMQConfig.CONSUMER_TAG, "*")

    // 获取起始消费位点
    val startOffset = tableOptions.get(FireRocketMQConf.ROCKET_STARTING_OFFSET)
    if (noEmpty(startOffset)) properties.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, startOffset)

    // 消费rocketmq埋点信息
    LineageManager.addMQDatasource("rocketmq", nameserver, topic, groupId, FOperation.SOURCE)

    val keyDeserialization = createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix)
    val valueDeserialization = createDeserialization(context, valueDecodingFormat, valueProjection, null)

    SourceFunctionProvider.of(new RocketMQSourceWithTag(new JsonDeserializationSchema(keyDeserialization, valueDeserialization), properties), false)
  }

}
