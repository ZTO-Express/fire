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

package com.zto.fire.common.conf

/**
 * 颜色预定义
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:01
 */
private[fire] object FirePS1Conf {
  // 颜色相关
  lazy val GREEN = "\u001B[32m"
  lazy val DEFAULT = "\u001B[0m"
  lazy val RED = "\u001B[31m"
  lazy val YELLOW = "\u001B[33m"
  lazy val BLUE = "\u001B[34m"
  lazy val PURPLE = "\u001B[35m"
  lazy val PINK = "\u001B[35m"
  // 字体相关
  lazy val HIGH_LIGHT = "\u001B[1m"
  lazy val ITALIC = "\u001B[3m"
  lazy val UNDER_LINE = "\u001B[4m"
  lazy val FLICKER = "\u001B[5m"

  /**
   * 包裹处理
   *
   * @param str
   * 原字符串
   * @param ps1
   * ps1
   * @return
   * wrap后的字符串
   */
  def wrap(str: String, ps1: String*): String = {
    val printStr = new StringBuilder()
    ps1.foreach(ps => {
      printStr.append(ps)
    })
    printStr.append(str + DEFAULT).toString()
  }
}