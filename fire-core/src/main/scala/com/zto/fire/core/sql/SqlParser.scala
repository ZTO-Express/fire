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

package com.zto.fire.core.sql

import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.conf.FireFrameworkConf._
import com.zto.fire.common.util.{LineageManager, Logging, SQLLineageManager, TableMeta, ThreadUtils}
import com.zto.fire.predef._

import java.util.concurrent.{CopyOnWriteArraySet, TimeUnit}

/**
 * 用于各引擎的SQL解析
 *
 * @author ChengLong 2021-6-18 16:28:50
 * @since 2.0.0
 */
@Internal
private[fire] trait SqlParser extends Logging {
  // 用于临时存放解析后的库表类
  protected[fire] lazy val tmpTableMap = new JHashMap[String, TableMeta]()
  // 用于存放按数据源归类后的所有血缘信息
  protected lazy val tableMetaSet = new CopyOnWriteArraySet[TableMeta]()
  protected[fire] lazy val hiveTableMap = new JConcurrentHashMap[String, Boolean]()
  protected lazy val buffer = new CopyOnWriteArraySet[String]()
  this.sqlParse

  /**
   * 周期性的解析SQL语句
   */
  @Internal
  protected def sqlParse: Unit = {
    if (lineageEnable) {
      ThreadUtils.scheduleWithFixedDelay({
        this.buffer.foreach(sql => this.sqlParser(sql))
        LineageManager.addTableMeta(this.tableMetaSet)
        this.clear
      }, lineageRunInitialDelay, lineageRunPeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 将解析后的血缘信息临时存放，并通过catalog进行归类后统一收集
   *
   * @param tableIdentifier
   * 库表名
   */
  @Internal
  protected def addTmpTableMeta(tableIdentifier: String, tmpTableMap: TableMeta): Unit = {
    this.tmpTableMap += (tableIdentifier -> tmpTableMap)
    this.collectTableMeta(tmpTableMap)
  }

  /**
   * 用于收集并按catalog归类数据源信息
   *
   * @param tableMeta 数据源
   */
  @Internal
  private def collectTableMeta(tableMeta: TableMeta): Unit = this.tableMetaSet += tableMeta

  /**
   * 清理解析后的SQL数据
   */
  @Internal
  private[this] def clear: Unit = {
    this.buffer.clear()
    this.tmpTableMap.clear()
    this.tableMetaSet.clear()
  }

  /**
   * 将待解析的SQL添加到buffer中
   */
  @Internal
  def sqlParse(sql: String): Unit = {
    if (lineageEnable && noEmpty(sql)) {
      SQLLineageManager.addStatement(sql)
      this.buffer += sql
    }
  }

  /**
   * 用于解析给定的SQL语句
   */
  @Internal
  def sqlParser(sql: String): Unit

  /**
   * SQL语法校验
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  @Internal
  def sqlLegal(sql: String): Boolean

  /**
   * 用于判断给定的表是否为临时表
   */
  @Internal
  def isTempView(tableIdentifier: TableIdentifier): Boolean

  /**
   * 用于判断给定的表是否为hive表
   */
  @Internal
  def isHiveTable(tableIdentifier: TableIdentifier): Boolean

  /**
   * 将库表名转为字符串
   */
  @Internal
  def tableIdentifier(dbName: String, tableName: String): String = s"$dbName.$tableName"
}
