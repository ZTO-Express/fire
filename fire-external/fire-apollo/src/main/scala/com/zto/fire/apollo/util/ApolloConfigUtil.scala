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

package com.zto.fire.apollo.util

import com.zto.fire.common.util.{Logging, PropUtils}
import org.apache.commons.lang3.StringUtils

import java.util.Properties

object ApolloConfigUtil extends Logging {

  private val props = new Properties()

  this.load()

  def load(): Unit = {

    var apolloEnv = System.getProperty(ApolloConstant.APOLLO_ENV)

    if(StringUtils.isNotBlank(apolloEnv)){
      apolloEnv = apolloEnv +  ApolloConstant.APOLLO_META_SUFFIX
    }else{
      apolloEnv = ApolloConstant.APOLLO_META_DEV
    }

    PropUtils.load(ApolloConstant.APOLLO_CONFIG_FILE)

    val appId = PropUtils.getString(ApolloConstant.APOLLO_APP_ID)
    val apolloMeta = PropUtils.getString(apolloEnv)

    if(StringUtils.isBlank(System.getProperty(ApolloConstant.APOLLO_APP_ID))) {
      System.setProperty(ApolloConstant.APOLLO_APP_ID, appId)
    }

    if(StringUtils.isBlank(System.getProperty(ApolloConstant.APOLLO_META))) {
      System.setProperty(ApolloConstant.APOLLO_META, apolloMeta)
    }

    val config = ConfigService.getAppConfig
    for (key <- config.getPropertyNames) {
      props.setProperty(key, config.getProperty(key, null))
    }

    val changeListener = new ConfigChangeListener() {
      override def onChange(changeEvent: ConfigChangeEvent): Unit = {
        logger.info("Changes for namespace {}", changeEvent.getNamespace)
        for (key <- changeEvent.changedKeys) {
          val change = changeEvent.getChange(key)
          props.setProperty(change.getPropertyName, change.getNewValue)
          logger.info("Change - key: {}, oldValue: {}, newValue: {}, changeType: {}", change.getPropertyName, change.getOldValue, change.getNewValue, change.getChangeType)
        }
      }
    }
    config.addChangeListener(changeListener)
  }

  /**
   * 返回配置
   * @return
   */
  def getProp(): Properties ={
    props
  }

  /**
   * 根据key获取配置信息
   *
   * @param key
   * 配置的key
   * @return
   * 配置的value
   */
  def getProperty(key: String, default: String = null): String = {
    props.getProperty(key, default)
  }

  /**
   * 获取字符串
   *
   * @param key
   * @return
   */
  def getString(key: String): String = {
    this.getProperty(key)
  }

  /**
   * 获取拼接后数值的配置字符串
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getString(key: String, keyNum: Int = 0, default: String = ""): String = {
    if (keyNum <= 1) {
      var value = this.getProperty(key)
      if (StringUtils.isBlank(value)) {
        value = this.getString(key + "1", default)
      }
      value
    } else {
      this.getString(key + keyNum, default)
    }
  }

  /**
   * 获取字符串，为空则取默认值
   *
   * @param key
   * @return
   */
  def getString(key: String, default: String): String = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value else default
  }

  /**
   * 获取整型数据
   *
   * @param key
   * @return
   */
  def getInt(key: String): Int = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toInt else -1
  }

  /**
   * 获取整型数据
   *
   * @param key
   * @return
   */
  def getInt(key: String, default: Int): Int = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toInt else default
  }

  /**
   * 获取拼接后数值的配置整数
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getInt(key: String, keyNum: Int = 0, default: Int): Int = {
    val value = this.getString(key, keyNum, default + "")
    if (StringUtils.isNotBlank(value)) value.toInt else default
  }

  /**
   * 获取长整型数据
   *
   * @param key
   * @return
   */
  def getLong(key: String): Long = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toLong else -1L
  }

  /**
   * 获取长整型数据
   *
   * @param key
   * @return
   */
  def getLong(key: String, default: Long): Long = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toLong else default
  }

  /**
   * 获取float型数据
   *
   * @param key
   * @return
   */
  def getFloat(key: String): Float = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toFloat else -1
  }

  /**
   * 获取float型数据
   *
   * @param key
   * @return
   */
  def getFloat(key: String, default: Float): Float = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toFloat else default
  }

  /**
   * 获取float型数据
   *
   * @param key
   * @return
   */
  def getDouble(key: String): Double = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toDouble else -1.0
  }

  /**
   * 获取float型数据
   *
   * @param key
   * @return
   */
  def getDouble(key: String, default: Double): Double = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toDouble else default
  }

  /**
   * 获取拼接后数值的配置长整数
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getLong(key: String, keyNum: Int = 0, default: Long): Long = {
    val value = this.getString(key, keyNum, default + "")
    if (StringUtils.isNotBlank(value)) value.toLong else default
  }

  /**
   * 获取布尔值数据
   */
  def getBoolean(key: String): Boolean = {
    this.getProperty(key, "false").toBoolean
  }

  /**
   * 获取布尔值数据
   */
  def getBoolean(key: String, default: Boolean): Boolean = {
    val value = this.getBoolean(key)
    if (value != null) value else default
  }

  /**
   * 获取拼接后数值的配置布尔值
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getBoolean(key: String, keyNum: Int = 0, default: Boolean): Boolean = {
    val value = this.getString(key, keyNum, default + "")
    if (StringUtils.isNotBlank(value)) value.toBoolean else default
  }

  def getEvn(env:String) {
    EnvUtils.transformEnv(env);
  }

}
