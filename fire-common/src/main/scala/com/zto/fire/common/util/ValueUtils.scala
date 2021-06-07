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

import java.util

import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

/**
 * 值校验工具，支持任意对象、字符串、集合、map、rdd、dataset是否为空的校验
 *
 * @since 0.4.1
 * @author ChengLong 2019-9-4 13:39:16
 */
private[fire] trait ValueCheck {

  /**
   * 值为空判断，支持任意类型
   *
   * @param params
   * 参数值
   * @return
   * true:empty false:not empty
   */
  def isEmpty(params: Any *): Boolean = {
    if (params == null || params.isEmpty) return true
    params.map {
      case null => true
      case str: String => StringUtils.isBlank(str)
      case array: Array[_] => array.isEmpty
      case collection: util.Collection[_] => collection.isEmpty
      case it: Iterable[_] => it.isEmpty
      case map: JMap[_, _] => map.isEmpty
      case _ => false
    }.count(_ == true) > 0
  }

  /**
   * 值为非空判断，支持任意类型
   *
   * @param param
   * 参数值
   * @return
   * true:not empty false:empty
   */
  def noEmpty(param: Any *): Boolean = !this.isEmpty(param: _*)

  /**
   * 参数非空约束（严格模式，进一步验证集合是否有元素）
   *
   * @param params  参数列表信息
   * @param message 异常信息
   */
  def requireNonEmpty(params: Any*)(implicit message: String = "参数不能为空，请检查."): Unit = {
    require(params != null && params.nonEmpty, message)

    var index = 0
    params.foreach(param => {
      index += 1
      param match {
        case null => require(param != null, msg(index, message))
        case str: String => require(StringUtils.isNotBlank(str), msg(index, message))
        case array: Array[_] => require(array.nonEmpty, msg(index, message))
        case collection: util.Collection[_] => require(!collection.isEmpty, msg(index, message))
        case it: Iterable[_] => require(it.nonEmpty, msg(index, message))
        case map: JMap[_, _] => require(map.nonEmpty, msg(index, message))
        case _ =>
      }
    })

    /**
     * 构建异常信息
     */
    def msg(index: Int, msg: String): String = s"第[ ${index} ]参数为空，异常信息：$message"
  }
}

/**
 * 用于单独调用的值校验工具类
 */
object ValueUtils extends ValueCheck
