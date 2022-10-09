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
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.EncodingFormat
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.DataTypeUtils
import org.apache.rocketmq.flink.common.selector.DefaultTopicSelector
import org.apache.rocketmq.flink.common.serialization.JsonSerializationSchema
import org.apache.rocketmq.flink.{RocketMQConfig, RocketMQSinkWithTag}

/**
 * 定义source table
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class RocketMQDynamicTableSink(physicalDataType: DataType,
                               keyDecodingFormat: EncodingFormat[SerializationSchema[RowData]],
                               valueDecodingFormat: EncodingFormat[SerializationSchema[RowData]],
                               keyProjection: Array[Int],
                               valueProjection: Array[Int],
                               keyPrefix: String,
                               tableOptions: JMap[String, String]) extends DynamicTableSink {


  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = ChangelogMode.insertOnly()

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
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

    // 获取tag信息
    val tag = tableOptions.get(FireRocketMQConf.ROCKET_CONSUMER_TAG)
    if (noEmpty(tag)) properties.setProperty(RocketMQConfig.CONSUMER_TAG, tag) else properties.setProperty(RocketMQConfig.CONSUMER_TAG, "*")

    // sink的并行度
    val sinkParallelism = tableOptions.getOrElse(FireRocketMQConf.ROCKET_SINK_PARALLELISM, null)

    // 消费rocketmq埋点信息
    LineageManager.addMQDatasource("rocketmq", nameserver, topic, "", FOperation.SINK)

    val keyDeserialization = createSerialization(context, keyDecodingFormat, keyProjection, keyPrefix)
    val valueDeserialization = createSerialization(context, valueDecodingFormat, valueProjection, null)
    val sink = new RocketMQSinkWithTag[RowData](new JsonSerializationSchema(topic, tag, valueDeserialization), new DefaultTopicSelector(topic), properties)
    SinkFunctionProvider.of(sink, if (noEmpty(sinkParallelism)) sinkParallelism.trim.toInt else null)
  }

  override def copy(): DynamicTableSink = new RocketMQDynamicTableSink(physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, tableOptions)

  override def asSummaryString(): String = "fire-rocketmq sink"

  /**
   * 创建反序列化器
   */
  def createSerialization(context: DynamicTableSink.Context, format: EncodingFormat[SerializationSchema[RowData]], projection: Array[Int], prefix: String): SerializationSchema[RowData] = {
    if (format == null) return null

    var physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection)
    if (noEmpty(prefix)) {
      physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix)
    }
    format.createRuntimeEncoder(context, physicalFormatDataType)
  }

}
