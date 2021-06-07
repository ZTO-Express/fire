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

import java.math.BigDecimal

/**
  * 数值类型常用操作工具类
  * Created by ChengLong on 2018-06-01.
  */
object NumberFormatUtils {

  /**
    * floor操作
    *
    * @param field
    * @return
    */
  def floor(field: Double): Int = {
    if (field == null) 0 else Math.floor(field).toInt
  }

  /**
    * 将Long转为Integer
    *
    * @param field
    * @return
    */
  def long2Int(field: java.lang.Long): java.lang.Integer = {
    if (field != null) {
      field.toInt
    } else {
      0
    }
  }

  /**
    * 将BigDecimal转为Long类型
    *
    * @param field
    * @return
    */
  def bigDecimal2Long(field: java.math.BigDecimal): java.lang.Long = {
    if (field != null) {
      field.longValue()
    } else {
      0L
    }
  }

  /**
    * 判断是否为空
    *
    * @param decimal
    * @return
    */
  def ifnull(decimal: java.math.BigDecimal, defaultVal: java.math.BigDecimal): java.math.BigDecimal = {
    if (decimal == null) defaultVal else decimal
  }

  /**
    * 类似于round，但不会四舍五入
    *
    * @param value
    * 目标值
    * @param scale
    * 精度
    * @return
    */
  def truncate(value: Double, scale: Int): Double = {
    if (value == null) {
      0.0
    } else {
      new BigDecimal(value).setScale(Math.abs(scale), BigDecimal.ROUND_HALF_UP).doubleValue()
    }
  }

  def truncate2(value: Double, scale: Int): Double = {
    if (value == null) {
      0.0
    } else if (scale == 0) {
      value.toLong
    } else {
      val tmp = Math.pow(10, Math.abs(scale))
      (value * tmp).asInstanceOf[Int] / tmp
    }
  }

  /**
    * 截取精度
    *
    * @param bigDecimal
    * @param scale
    * 精度
    * @return
    */
  def truncateDecimal(bigDecimal: java.math.BigDecimal, scale: Int): java.math.BigDecimal = {
    if (bigDecimal == null) {
      new java.math.BigDecimal("0").setScale(scale, BigDecimal.ROUND_HALF_UP)
    } else {
      bigDecimal.setScale(scale, BigDecimal.ROUND_HALF_UP)
    }
  }

}
