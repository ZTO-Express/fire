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

import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
 * SQL相关工具类
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-26 15:09
 */
object SQLUtils {
  private[this] val beforeWorld = "(?i)(from|join|update|into table|table|into|exists|desc|like|if)"
  private[this] val reg = s"${beforeWorld}\\s+(\\w+\\.\\w+|\\w+)".r

  /**
   * 利用正则表达式解析SQL中用到的表名
   */
  def tableParse(sql: String): ListBuffer[String] = {
    require(StringUtils.isNotBlank(sql), "sql语句不能为空")

    val tables = ListBuffer[String]()
    // 找出所有beforeWorld中定义的关键字匹配到的后面的表名
    reg.findAllMatchIn(sql.replace("""`""", "")).foreach(tableName => {
      // 将匹配到的数据剔除掉beforeWorld中定义的关键字
      val name = tableName.toString().replaceAll(s"${beforeWorld}\\s+", "").trim
      if (StringUtils.isNotBlank(name)) tables += name
    })

    tables
  }
  
}
