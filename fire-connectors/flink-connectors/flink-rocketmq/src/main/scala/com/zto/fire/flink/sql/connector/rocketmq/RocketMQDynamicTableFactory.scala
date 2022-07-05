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

import com.zto.fire.flink.sql.connector.rocketmq.RocketMQOptions._
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.table.connector.format.{DecodingFormat, EncodingFormat}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories._
import com.zto.fire.predef._

/**
 * sql connector的source与sink创建工厂
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class RocketMQDynamicTableFactory extends DynamicTableSourceFactory with DynamicTableSinkFactory {
  val IDENTIFIER = "fire-rocketmq"

  override def factoryIdentifier(): String = this.IDENTIFIER

  private def getKeyDecodingFormat(helper: FactoryUtil.TableFactoryHelper): DecodingFormat[DeserializationSchema[RowData]] = {
    helper.discoverDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)
  }

  private def getValueDecodingFormat(helper: FactoryUtil.TableFactoryHelper): DecodingFormat[DeserializationSchema[RowData]] = {
    helper.discoverDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)
  }

  private def getKeyEncodingFormat(helper: FactoryUtil.TableFactoryHelper): EncodingFormat[SerializationSchema[RowData]] = {
    helper.discoverEncodingFormat(classOf[SerializationFormatFactory], FactoryUtil.FORMAT)
  }

  private def getValueEncodingFormat(helper: FactoryUtil.TableFactoryHelper): EncodingFormat[SerializationSchema[RowData]] = {
    helper.discoverEncodingFormat(classOf[SerializationFormatFactory], FactoryUtil.FORMAT)
  }

  /**
   * 必填参数列表
   */
  override def requiredOptions(): JSet[ConfigOption[_]] = {
    val set = new JHashSet[ConfigOption[_]]
    set.add(TOPIC)
    set.add(PROPS_BOOTSTRAP_SERVERS)
    set.add(PROPS_GROUP_ID)
    set
  }

  /**
   * 可选的参数列表
   */
  override def optionalOptions(): JSet[ConfigOption[_]] = {
    val optionalOptions = new JHashSet[ConfigOption[_]]
    optionalOptions
  }


  /**
   * 创建rocketmq table source
   */
  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    val tableOptions = helper.getOptions
    val keyDecodingFormat = this.getKeyDecodingFormat(helper)
    val valueDecodingFormat = this.getValueDecodingFormat(helper)
    val withOptions = context.getCatalogTable.getOptions
    val physicalDataType = context.getCatalogTable.getSchema.toPhysicalRowDataType
    val keyProjection = createKeyFormatProjection(tableOptions, physicalDataType)
    val valueProjection = createValueFormatProjection(tableOptions, physicalDataType)
    val keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null)

    new RocketMQDynamicTableSource(physicalDataType,
      keyDecodingFormat,
      valueDecodingFormat,
      keyProjection,
      valueProjection,
      keyPrefix,
      withOptions)
  }

  /**
   * 创建rocketmq table sink
   */
  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val tableOptions = helper.getOptions()
    val keyDecodingFormat = this.getKeyEncodingFormat(helper)
    val valueDecodingFormat = this.getValueEncodingFormat(helper)
    val physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType()
    val keyProjection = RocketMQOptions.createKeyFormatProjection(tableOptions, physicalDataType)
    val valueProjection = RocketMQOptions.createValueFormatProjection(tableOptions, physicalDataType)
    val keyPrefix = tableOptions.getOptional(RocketMQOptions.KEY_FIELDS_PREFIX).orElse(null)
    val parallelism = tableOptions.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(8)
    val withOptions = context.getCatalogTable.getOptions

    new RocketMQDynamicTableSink(physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, withOptions)
  }
}
