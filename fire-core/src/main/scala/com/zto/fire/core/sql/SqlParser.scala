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

import com.zto.fire.common.conf.FireFrameworkConf.{buriedPointDatasourceEnable, buriedPointDatasourceInitialDelay, buriedPointDatasourcePeriod}
import com.zto.fire.common.util.{DatasourceManager, Logging, TableMeta, ThreadUtils}
import com.zto.fire.predef._

import java.util.concurrent.{CopyOnWriteArraySet, TimeUnit}

/**
 * 用于各引擎的SQL解析
 *
 * @author ChengLong 2021-6-18 16:28:50
 * @since 2.0.0
 */
trait SqlParser extends Logging {
  // 用于临时存放解析后的库表类
  protected lazy val tmpTableMap = new JHashMap[String, TableMeta]()
  // 用于存放按数据源归类后的所有血缘信息
  protected lazy val tableMetaSet = new CopyOnWriteArraySet[TableMeta]()
  protected[fire] lazy val hiveTableMap = new JConcurrentHashMap[String, Boolean]()
  protected lazy val buffer = new CopyOnWriteArraySet[String]()
  this.sqlParse

  /**
   * 周期性的解析SQL语句
   */
  protected def sqlParse: Unit = {
    if (buriedPointDatasourceEnable) {
      ThreadUtils.scheduleWithFixedDelay({
        this.buffer.foreach(sql => this.sqlParser(sql))
        DatasourceManager.addTableMeta(this.tableMetaSet)
        this.clear
      }, buriedPointDatasourceInitialDelay, buriedPointDatasourcePeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 将解析后的血缘信息临时存放，并通过catalog进行归类后统一收集
   *
   * @param tableIdentifier
   * 库表名
   */
  protected def addTmpTableMeta(tableIdentifier: String, tmpTableMap: TableMeta): Unit = {
    this.tmpTableMap += (tableIdentifier -> tmpTableMap)
    this.collectTableMeta(tmpTableMap)
  }

  /**
   * 用于收集并按catalog归类数据源信息
   *
   * @param tableMeta 数据源
   */
  private def collectTableMeta(tableMeta: TableMeta): Unit = this.tableMetaSet += tableMeta

  /**
   * 清理解析后的SQL数据
   */
  private[this] def clear: Unit = {
    this.buffer.clear()
    this.tmpTableMap.clear()
    this.tableMetaSet.clear()
  }

  /**
   * 将待解析的SQL添加到buffer中
   */
  def sqlParse(sql: String): Unit = {
    if (buriedPointDatasourceEnable && noEmpty(sql)) {
      this.buffer += sql
    }
  }

  /**
   * 用于解析给定的SQL语句
   */
  def sqlParser(sql: String): Unit

  /**
   * 用于判断给定的表是否为临时表
   */
  def isTempView(dbName: String = null, tableName: String): Boolean

  /**
   * 用于判断给定的表是否为hive表
   */
  def isHiveTable(dbName: String = null, tableName: String): Boolean

  /**
   * 将库表名转为字符串
   */
  def tableIdentifier(dbName: String, tableName: String): String = s"$dbName.$tableName"
}
