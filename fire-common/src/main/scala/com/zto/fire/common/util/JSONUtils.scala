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

import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.apache.htrace.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import org.apache.htrace.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.htrace.fasterxml.jackson.annotation.PropertyAccessor
import org.apache.htrace.fasterxml.jackson.core.JsonParser
import org.apache.htrace.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}

import scala.reflect.ClassTag
import scala.util.Try

/**
 * json处理工具类，基于jackson封装
 *
 * @author ChengLong 2021年4月14日09:27:37
 * @since fire 2.0.0
 */
object JSONUtils {

  private[this] lazy val objectMapperLocal = new ThreadLocal[ObjectMapper]() {
    override def initialValue(): ObjectMapper = newObjectMapperWithDefaultConf
  }

  /**
   * 创建一个新的ObjectMapper实例
   */
  def newObjectMapper: ObjectMapper = new ObjectMapper

  /**
   * 创建一个新的ObjectMapper实例，并设置一系列默认的属性
   */
  def newObjectMapperWithDefaultConf: ObjectMapper = {
    this.newObjectMapper
      .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      .configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, true)
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      .setSerializationInclusion(Include.ALWAYS)
      .setVisibility(PropertyAccessor.ALL, Visibility.ANY)
  }

  /**
   * 从线程局部变量中获取对应的ObjectMapper对象实例
   */
  def getObjectMapper: ObjectMapper = this.objectMapperLocal.get()

  /**
   * 将给定的对象解析成json字符串
   *
   * @param obj 任意的对象实例
   * @return json字符串
   */
  def toJSONString(obj: Object): String = this.getObjectMapper.writeValueAsString(obj)

  /**
   * 将给定的json字符串转为T类型对象实例
   *
   * @param json
   * json字符串
   * @tparam T
   * 目标泛型类型
   * @return
   * 目标对象实例
   */
  def parseObject[T: ClassTag](json: String): T = this.getObjectMapper.readValue[T](json, getParamType[T])

  /**
   * 将给定的json字符串转为T类型对象实例
   *
   * @param json
   * json字符串
   * @param valueType
   * 目标类型
   * @tparam T
   * 目标泛型类型
   * @return
   * 目标对象实例
   */
  def parseObject[T](json: String, valueType: Class[T]): T = this.getObjectMapper.readValue[T](json, valueType)


  /**
   * 用于判断给定的字符串是否为合法的json
   *
   * @param json
   * 待校验的字符串
   * @param strictMode
   * 检查模式，如果是true则会进行严格的检查，会牺牲部分性能，如果为false，则只进行简单的检查，性能较好
   * @return
   * true: 合法的字符串 false：非法的json字符串
   */
  def isJson(json: String, strictMode: Boolean = true): Boolean = {
    if (strictMode) {
      Try {
        try parseObject[JMap[Object, Object]](json)
      }.isSuccess
    } else {
      val jsonStr = StringUtils.trim(json)
      if (StringUtils.isBlank(jsonStr)) return false
      jsonStr.startsWith("{") && jsonStr.endsWith("}")
    }
  }

  /**
   * 用于判断给定的字符串是否为合法的jsonarray
   *
   * @param jsonArray
   * 待校验的字符串
   * @param strictMode
   * 检查模式，如果是true则会进行严格的检查，会牺牲部分性能，如果为false，则只进行简单的检查，性能较好
   * @return
   * true: 合法的字符串 false：非法的json字符串
   */
  def isJsonArray(jsonArray: String, strictMode: Boolean = true): Boolean = {
    if (strictMode) {
      Try {
        try parseObject[JList[Object]](jsonArray)
      }.isSuccess
    } else {
      val jsonArrayStr = StringUtils.trim(jsonArray)
      if (StringUtils.isBlank(jsonArrayStr)) return false
      jsonArrayStr.startsWith("[") && jsonArrayStr.endsWith("]")
    }
  }

  /**
   * 用于快速判断给定的字符串是否为合法的JsonArray或json
   * 注：不会验证每个field的合法性，仅做简单校验
   *
   * @param json
   * 待校验的字符串
   * @return
   * true: 合法的字符串 false：非法的json字符串
   */
  def isLegal(json: String, strictMode: Boolean = true): Boolean = this.isJson(json, strictMode) || this.isJsonArray(json, strictMode)

  /**
   * 用于快速判断给定的字符串是否为合法的JsonArray或json
   * 注：不会验证每个field的合法性，仅做简单校验
   *
   * @param json
   * 待校验的字符串
   * @return
   * true: 合法的字符串 false：非法的json字符串
   */
  def checkJson(json: String, strictMode: Boolean = true): Boolean = this.isLegal(json, strictMode)

  /**
   * 解析JSON，并获取指定key对应的值
   *
   * @param json json字符串
   * @param key  json的key
   * @return value
   */
  def getValue[T: ClassTag](json: String, key: String, defaultValue: T): T = {
    if (!this.isLegal(json)) return defaultValue
    val map = this.parseObject[JHashMap[String, Object]](json)
    map.getOrElse(key, defaultValue).asInstanceOf[T]
  }

}
