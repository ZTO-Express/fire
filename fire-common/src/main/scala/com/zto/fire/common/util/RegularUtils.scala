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

/**
 * 常用的正则表达式
 *
 * @author ChengLong 2021-5-28 11:14:19
 * @since fire 2.0.0
 */
object RegularUtils {
  // 用于匹配纯数值的表达式
  lazy val numeric = "(^[1-9]\\d*\\.?\\d*$)|(^0\\.\\d*[1-9]$)".r
  // 用于匹配字符串中以数值开头的数值
  lazy val numericPrefix = "(^[1-9]\\d*\\.?\\d*)|(^0\\.\\d*[1-9])".r
  // 用于匹配字符串中以固定的字母+空白符结尾
  lazy val unitSuffix = "[a-zA-Z]+\\s*$".r
}
