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

package com.zto.fire.flink.ext.stream

import com.zto.fire.common.util.{Logging, PropUtils}
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.{noEmpty, requireNonEmpty}
import com.zto.fire._

/**
 * Flink SQL扩展类
 *
 * @author ChengLong 2021-4-23 10:36:49
 * @since 2.0.0
 */
class SQLExt(sql: String) extends Logging {
  // 用于匹配Flink SQL中的with表达式
  private[this] lazy val withPattern = """(with|WITH)\s*\(([\s\S]*)(\)|;)$""".r
  // 用于匹配Flink SQL中的create语句
  private[this] lazy val createTablePattern = """^\s*(create|CREATE)\s+(table|TABLE)""".r
  private[this] lazy val withMapCache = new JConcurrentHashMap[Int, Map[String, String]]()

  /**
   * 将给定的不包含with表达式的Flink SQL添加with表达式
   *
   * @param keyNum
   * with表达式在配置文件中声明的keyNum，小于零时，则表示不拼接with表达式
   * @return
   * 组装了with表达式的Flink SQL文本
   */
  def with$(keyNum: Int = 1): String = {
    requireNonEmpty(sql, "sql语句不能为空！")

    // 未开启with表达式替换或keyNum不合法，则直接返回
    if (!FireFlinkConf.sqlWithReplaceModeEnable || keyNum < 1) return sql

    // 仅匹配create table语句，进行with表达式处理
    val createTableMatcher = this.createTablePattern.findFirstIn(sql)
    // 非create table语句，直接返回
    if (createTableMatcher.isEmpty) return sql

    // 匹配sql中的with表达式，如果sql中已经定义了with表达式，则不做替换
    val withMatcher = withPattern.findFirstIn(sql)
    if (withMatcher.isDefined) return sql

    // 从配置文件中获取指定keyNum的with参数，keyNum确定唯一的sql语句
    val withMap = withMapCache.getOrElse(keyNum, PropUtils.sliceKeysByNum(FireFlinkConf.FLINK_SQL_WITH_PREFIX, keyNum))
    // 当sql语句中没有指定with表达式并且没有配置with参数，则进行提示
    if (withMap.isEmpty && withMatcher.isEmpty) {
      this.logger.error(s"未搜索到keyNum=${keyNum}对应的sql配置列表，请以${FireFlinkConf.FLINK_SQL_WITH_PREFIX}开头，以${keyNum}结尾进行配置")
      return sql
    }

    // 替换create table语句中的with表达式，并返回最终的sql
    val fixSql = if (withMatcher.isDefined) withPattern.replaceAllIn(sql, "") else sql
    val finalSQL = buildWith(fixSql, withMap)
    if (FireFlinkConf.sqlLogEnable) logger.debug(s"完整SQL语句：$finalSQL")
    finalSQL
  }

  /**
   * 根据给定的配置列表构建Flink SQL with表达式
   *
   * @param map
   * Flink SQL with配置列表
   * @return
   * with sql表达式
   */
  private[fire] def buildWith(sql: String, map: Map[String, String]): String = {
    val withSql = new StringBuilder()
    withSql.append(sql).append("WITH (\n")
    map.filter(conf => noEmpty(conf, conf._1, conf._2)).foreach(conf => {
      withSql
        .append(s"""\t'${conf._1}'=""")
        .append(s"'${conf._2}'")
        .append(",\n")
    })
    withSql.substring(0, withSql.length - 2) + "\n)"
  }
}