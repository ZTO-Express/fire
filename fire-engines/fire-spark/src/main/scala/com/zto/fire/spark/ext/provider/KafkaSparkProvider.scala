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

package com.zto.fire.spark.ext.provider

import com.zto.fire.common.conf.FireKafkaConf
import com.zto.fire.common.util.{KafkaUtils, LogUtils}
import com.zto.fire.spark.util.SparkUtils
import com.zto.fire.{requireNonEmpty, retry, _}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

/**
 * 为扩展层提供Kafka相关的API
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:43
 */
trait KafkaSparkProvider extends SparkProvider {
  import spark.implicits._

  /**
   * 消费kafka中的json数据，并解析成json字符串
   *
   * @param extraOptions
   * 消费kafka额外的参数，如果有key同时出现在配置文件中和extraOptions中，将被extraOptions覆盖
   * @param keyNum
   * 配置文件中key的数字后缀
   * @return
   * 转换成json字符串后的Dataset
   */
  def loadKafka(extraOptions: Map[String, String] = null, keyNum: Int = 1): Dataset[(String, String)] = {
    val extraOptionsMap = new scala.collection.mutable.HashMap[String, String]
    if (extraOptions != null && extraOptions.nonEmpty) extraOptionsMap ++= extraOptions

    val confGroupId = FireKafkaConf.kafkaGroupId(keyNum)
    val groupId = if (StringUtils.isNotBlank(confGroupId)) confGroupId else spark.sparkContext.appName
    extraOptionsMap += ("group.id" -> groupId)

    val finalBrokers = FireKafkaConf.kafkaBrokers(keyNum)
    if (StringUtils.isNotBlank(finalBrokers)) extraOptionsMap += ("kafka.bootstrap.servers" -> finalBrokers)
    require(extraOptionsMap.contains("kafka.bootstrap.servers"), s"kafka bootstrap.servers不能为空，请在配置文件中指定：spark.kafka.brokers.name$keyNum")

    val topics = FireKafkaConf.kafkaTopics()
    if (StringUtils.isNotBlank(topics)) extraOptionsMap += ("subscribe" -> topics)
    require(extraOptionsMap.contains("subscribe"), s"kafka topic不能为空，请在配置文件中指定：spark.kafka.topics$keyNum")

    // 以spark.kafka.conf.开头的配置优先级最高
    val configMap = FireKafkaConf.kafkaConfMap(keyNum)
    extraOptionsMap ++= configMap
    LogUtils.logMap(this.logger, extraOptionsMap.toMap, s"Kafka client configuration. keyNum=$keyNum.")

    val kafkaReader = spark.readStream
      .format("kafka")
      .options(extraOptionsMap)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value")
      .as[(String, String)]
    kafkaReader
  }

  /**
   * 消费kafka中的json数据，并按照指定的schema解析成目标类型
   *
   * @param schemaClass
   * json对应的javabean类型
   * @param extraOptions
   * 消费kafka额外的参数
   * @param parseAll
   * 是否解析所有字段信息
   * @param isMySQL
   * 是否为mysql解析的消息
   * @param fieldNameUpper
   * 字段名称是否为大写
   * @return
   * 转换成json字符串后的Dataset
   */
  def loadKafkaParse(schemaClass: Class[_],
                     extraOptions: Map[String, String] = null,
                     parseAll: Boolean = false,
                     isMySQL: Boolean = true,
                     fieldNameUpper: Boolean = false, keyNum: Int = 1): DataFrame = {
    val kafkaDataset = this.loadKafka(extraOptions, keyNum)
    val schemaDataset = kafkaDataset.select(from_json($"value", SparkUtils.buildSchema2Kafka(schemaClass, parseAll, isMySQL, fieldNameUpper)).as("data"))
    if (parseAll)
      schemaDataset.select("data.*")
    else
      schemaDataset.select("data.after.*")
  }

  /**
   * 消费kafka中的json数据，并自动解析json数据，将解析后的数据注册到tableName所指定的临时表中
   *
   * @param tableName
   * 解析后的数据存放的临时表名，默认名为kafka
   * @param extraOptions
   * 消费kafka额外的参数
   * @return
   * 转换成json字符串后的Dataset
   */
  def loadKafkaParseJson(tableName: String = "kafka",
                         extraOptions: Map[String, String] = null,
                         keyNum: Int = 1): DataFrame = {
    val msg = retry(5, 1000) {
      KafkaUtils.getMsg(FireKafkaConf.kafkaBrokers(keyNum), FireKafkaConf.kafkaTopics(keyNum), null)
    }
    requireNonEmpty(msg, s"获取样例消息失败！请重启任务尝试重新获取，并保证topic[${FireKafkaConf.kafkaTopics(keyNum)}]持续的有新消息。")
    val jsonDS = this.spark.createDataset(Seq(msg))(Encoders.STRING)
    val jsonDF = this.spark.read.json(jsonDS)

    val kafkaDataset = this.loadKafka(extraOptions, keyNum)
    val schemaDataset = kafkaDataset.select(from_json($"value", jsonDF.schema).as(tableName)).select(s"${tableName}.*")
    schemaDataset.createOrReplaceTempView(tableName)
    schemaDataset
  }

  /**
   * 解析DStream中每个rdd的json数据，并转为DataFrame类型
   *
   * @param schema
   * 目标DataFrame类型的schema
   * @param isMySQL
   * 是否为mysql解析的消息
   * @param fieldNameUpper
   * 字段名称是否为大写
   * @param parseAll
   * 是否需要解析所有字段信息
   * @return
   */
  def kafkaJson2DFV(rdd: RDD[String], schema: Class[_], parseAll: Boolean = false, isMySQL: Boolean = true, fieldNameUpper: Boolean = false): DataFrame = {
    rdd.kafkaJson2DFV(schema, parseAll, isMySQL, fieldNameUpper)
  }

  /**
   * 解析DStream中每个rdd的json数据，并转为DataFrame类型
   *
   * @param schema
   * 目标DataFrame类型的schema
   * @param isMySQL
   * 是否为mysql解析的消息
   * @param fieldNameUpper
   * 字段名称是否为大写
   * @param parseAll
   * 是否解析所有字段信息
   * @return
   */
  def kafkaJson2DF(rdd: RDD[ConsumerRecord[String, String]], schema: Class[_], parseAll: Boolean = false, isMySQL: Boolean = true, fieldNameUpper: Boolean = false): DataFrame = {
    rdd.kafkaJson2DF(schema, parseAll, isMySQL, fieldNameUpper)
  }
}
