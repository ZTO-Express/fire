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

import com.zto.fire.flink.sql.connector.rocketmq.RocketMQOptions.ValueFieldsStrategy.ValueFieldsStrategy
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks

import java.util
import java.util.Properties
import java.util.stream.IntStream
import com.zto.fire.predef._

import scala.collection.{JavaConversions, JavaConverters}

/**
 * RocketMQ connector支持的with参数
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
object RocketMQOptions {
  val PROPERTIES_PREFIX = "rocket.conf."

  val TOPIC: ConfigOption[String] = ConfigOptions
    .key("topic")
    .stringType
    .noDefaultValue
    .withDescription("Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. Option 'topic' is required for sink.")

  val PROPS_BOOTSTRAP_SERVERS: ConfigOption[String] = ConfigOptions
    .key("properties.bootstrap.servers")
    .stringType
    .noDefaultValue
    .withDescription("Required RocketMQ server connection string")

  val PROPS_GROUP_ID: ConfigOption[String] = ConfigOptions
    .key("properties.group.id")
    .stringType.noDefaultValue
    .withDescription("Required consumer group in RocketMQ consumer, no need for v producer.")

  val KEY_FIELDS_PREFIX: ConfigOption[String] =
    ConfigOptions.key("key.fields-prefix")
      .stringType()
      .noDefaultValue()
      .withDescription(
        s"""
          |Defines a custom prefix for all fields of the key format to avoid name clashes with fields of the value format.
          |By default, the prefix is empty. If a custom prefix is defined, both the table schema and '${ValueFieldsStrategy.ALL}'
          |will work with prefixed names. When constructing the data type of the key format, the prefix will be removed and the
          |non-prefixed names will be used within the key format. Please note that this option requires that must be '${ValueFieldsStrategy.EXCEPT_KEY}'.
          |""".stripMargin)

  val KEY_FIELDS: ConfigOption[JList[String]] =
    ConfigOptions.key("key.fields")
      .stringType()
      .asList()
      .defaultValues()
      .withDescription(
        """
          |Defines an explicit list of physical columns from the table schema that configure the data type for the key format.
          |By default, this list is empty and thus a key is undefined.
          |""".stripMargin)

  val VALUE_FIELDS_INCLUDE: ConfigOption[ValueFieldsStrategy] =
    ConfigOptions.key("value.fields-include")
      .defaultValue(ValueFieldsStrategy.ALL)
      .withDescription(
        """
          |Defines a strategy how to deal with key columns in the data type of
          |the value format. By default, 'ValueFieldsStrategy.ALL' physical
          |columns of the table schema will be included in the value format which
          |means that key columns appear in the data type for both the key and value format.
          |""".stripMargin)

  val FORMAT_SUFFIX = ".format"

  val KEY_FORMAT: ConfigOption[String] =
    ConfigOptions.key("key" + FORMAT_SUFFIX)
      .stringType()
      .noDefaultValue()
      .withDescription("Defines the format identifier for encoding key data. The identifier is used to discover a suitable format factory.")

  val VALUE_FORMAT: ConfigOption[String] =
    ConfigOptions.key("value" + FORMAT_SUFFIX)
      .stringType()
      .noDefaultValue()
      .withDescription("Defines the format identifier for encoding value data. The identifier is used to discover a suitable format factory.")

  object ValueFieldsStrategy extends Enumeration {
    type ValueFieldsStrategy = Value
    val ALL, EXCEPT_KEY = Value
  }

  def createKeyFormatProjection(options: ReadableConfig, physicalDataType: DataType): Array[Int] = {
    val physicalType = physicalDataType.getLogicalType

    val optionalKeyFormat = options.getOptional(RocketMQOptions.KEY_FORMAT)
    val optionalKeyFields = options.getOptional(RocketMQOptions.KEY_FIELDS)

    if (!optionalKeyFormat.isPresent && optionalKeyFields.isPresent) {
      throw new ValidationException(s"The option '${RocketMQOptions.KEY_FIELDS.key}' can only be declared if a key format is defined using '${RocketMQOptions.KEY_FORMAT.key}'.")
    } else if (optionalKeyFormat.isPresent && (!optionalKeyFields.isPresent || optionalKeyFields.get.size == 0)) {
      throw new ValidationException(s"A key format '${RocketMQOptions.KEY_FORMAT.key}' requires the declaration of one or more of key fields using '${RocketMQOptions.KEY_FIELDS.key}'.")
    }

    if (!optionalKeyFormat.isPresent) return new Array[Int](0)
    val keyPrefix = options.getOptional(RocketMQOptions.KEY_FIELDS_PREFIX).orElse("")
    val keyFields = JavaConversions.asScalaBuffer(optionalKeyFields.get)
    val physicalFields = LogicalTypeChecks.getFieldNames(physicalType)
    keyFields.map((keyField: String) => {
      def foo(keyField: String): Int = {
        val pos = physicalFields.indexOf(keyField)
        // check that field name exists
        if (pos < 0) throw new ValidationException(s"Could not find the field '${keyField}' in the table schema for usage in the key format. A key field must be a regular, physical column. The following columns can be selected in the '${RocketMQOptions.KEY_FIELDS.key}' option:\n${physicalFields}")
        // check that field name is prefixed correctly
        if (!keyField.startsWith(keyPrefix)) throw new ValidationException(s"All fields in '${RocketMQOptions.KEY_FIELDS.key}' must be prefixed with '${keyPrefix}' when option '${RocketMQOptions.KEY_FIELDS_PREFIX.key}' is set but field '${keyField}' is not prefixed.")
        pos
      }

      foo(keyField)
    }).toArray
  }

  def createValueFormatProjection(options: ReadableConfig, physicalDataType: DataType): Array[Int] = {
    val physicalType = physicalDataType.getLogicalType

    val physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType)
    // val physicalFields = IntStream.range(0, physicalFieldCount)
    val physicalFields = (1 until physicalFieldCount).toArray

    val keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("")

    val strategy = options.get(VALUE_FIELDS_INCLUDE);
    if (strategy == ValueFieldsStrategy.ALL) {
      if (keyPrefix.nonEmpty) {
        throw new ValidationException(s"A key prefix is not allowed when option '${VALUE_FIELDS_INCLUDE.key()}' is set to '${ValueFieldsStrategy.ALL}'. Set it to '${ValueFieldsStrategy.EXCEPT_KEY}' instead to avoid field overlaps.")
      }
      return physicalFields
    } else if (strategy == ValueFieldsStrategy.EXCEPT_KEY) {
      val keyProjection = createKeyFormatProjection(options, physicalDataType);
      return physicalFields.filter(pos => !keyProjection.contains(pos))
    }
    throw new TableException(s"Unknown value fields strategy:$strategy");
  }

  /**
   * 是否存在以properties.开头的参数
   */
  private def hasRocketMQClientProperties(tableOptions: util.Map[String, String]) = {
    JavaConversions.mapAsScalaMap(tableOptions)
      .keySet
      .filter((k: String) => k.startsWith(PROPERTIES_PREFIX)).size > 0
  }

  /**
   * 获取以rocket.conf.开头的所有的参数
   */
  def getRocketMQProperties(tableOptions: util.Map[String, String]): Properties = {
    val rocketMQProperties = new Properties
    if (hasRocketMQClientProperties(tableOptions)) JavaConversions.mapAsScalaMap(tableOptions).keySet.filter((key: String) => key.startsWith(PROPERTIES_PREFIX)).foreach((key: String) => {
      def foo(key: String): Unit = {
        val value = tableOptions.get(key)
        val subKey = key.substring(PROPERTIES_PREFIX.length)
        rocketMQProperties.put(subKey, value)
      }

      foo(key)
    })
    rocketMQProperties
  }
}
