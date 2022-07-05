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

package com.zto.fire.common.ext

import com.zto.fire.common.conf.FireFrameworkConf

import java.util.regex.Pattern

/**
 * scala相关扩展
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-04 10:32
 */
trait ScalaExt {
  // 用于缓存转为驼峰标识的字符串与转换前的字符串的映射关系
  private[this] lazy val humpMap = collection.mutable.Map[String, String]()
  private[this] var printCount = 0L

  /**
   * String API扩展
   */
  implicit class StringExt[K, V](str: String) {
    // 用于匹配带有下划线字符串的正则
    private[this] lazy val humpPattern = Pattern.compile("(.*)_(\\w)(.*)")
    private[this] lazy val maxHumpMapSize = 10000

    /**
     * 数据表字段名转换为驼峰式名字的实体类属性名
     *
     * @return 转换后的驼峰式命名
     */
    def toHump: String = {
      val matcher = humpPattern.matcher(str)
      val humpStr = if (matcher.find) {
        (matcher.group(1) + matcher.group(2).toUpperCase + matcher.group(3)).toHump
      } else str
      if (humpMap.size <= this.maxHumpMapSize) humpMap += (humpStr -> str)
      humpStr
    }

    /**
     * 驼峰式的实体类属性名转换为数据表字段名
     *
     * @return 转换后的以"_"分隔的数据表字段名
     */
    def unHump: String = humpMap.getOrElse(str, str.replaceAll("[A-Z]", "_$0").toLowerCase)
  }

  /**
   * print行数限制
   */
  private[this] def printLimit(x: Any)(fun: Any => Unit): Unit = {
    this.printCount += 1
    if (FireFrameworkConf.printLimit <= 0 || this.printCount <= FireFrameworkConf.printLimit) {
      fun(x)
    } else if (this.printCount <= FireFrameworkConf.printLimit * 1.1){
      Console.println(s"使用print打印行数超过fire.print.limit配置的${FireFrameworkConf.printLimit}条，生产环境请不要打印过多数据！")
    }
  }

  /** Prints an object to `out` using its `toString` method.
   *
   * @param x the object to print; may be null.
   * @group console-output
   */
  def print(x: Any): Unit = this.printLimit(x)(x => Console.print(x))

  /** Prints out an object to the default output, followed by a newline character.
   *
   * @param x the object to print.
   * @group console-output
   */
  def println(x: Any): Unit = this.printLimit(x)(x => Console.println(x))
}

