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

package com.zto.fire.spark.sql

import com.zto.fire.common.anno.Internal
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.util.{ExceptionBus, TableMeta}
import com.zto.fire.core.sql.SqlParser
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import com.zto.fire.{isEmpty, tryWithLog}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.CreateTable

import scala.collection.mutable
import scala.util.Try


/**
 * Spark SQL解析器，用于解析Spark SQL语句中的库、表、分区、操作类型等信息
 *
 * @author ChengLong 2021-6-18 16:31:04
 * @since 2.0.0
 */
object SparkSqlParser extends SqlParser {
  private lazy val spark = SparkSingletonFactory.getSparkSession
  private lazy val catalog = spark.catalog

  /**
   * 用于判断给定的表是否为hive表
   *
   * @param tableIdentifier 库表
   */
  def isHiveTableDatasource(tableIdentifier: (String, String)): Datasource = {
    val isHive = this.isHiveTable(tableIdentifier._1, tableIdentifier._2)
    if (isHive) Datasource.HIVE else Datasource.VIEW
  }

  /**
   * 用于判断给定的表是否为临时表
   */
  override def isTempView(dbName: String = null, tableName: String): Boolean = {
    try {
      val table = this.catalog.getTable(dbName, tableName)
      table.isTemporary
    } catch {
      case _ => false
    }
  }

  /**
   * 用于判断给定的表是否为hive表
   */
  override def isHiveTable(dbName: String = null, tableName: String): Boolean = true
  /**
   * 用于解析SparkSql中的库表信息
   */
  override def sqlParser(sql: String): Unit = {
    if (isEmpty(sql)) return
    tryWithLog {
      this.logger.debug(s"开始解析sql语句：$sql")
      val logicalPlan = this.spark.sessionState.sqlParser.parsePlan(sql)
      this.sqlQueryParser(logicalPlan, sql)
      this.ddlParser(logicalPlan, sql)
    } (this.logger, catchLog = s"可忽略异常：实时血缘解析SQL报错，SQL：\n${sql}")
  }

  /**
   * 用于解析查询sql中的库表信息
   */
  def sqlQueryParser(logicalPlan: LogicalPlan, sql: String = ""): Unit = {

  }

  /**
   * 获取库表名
   *
   * @param tableName 解析后的表信息
   */
  @Internal
  private[this] def tableName(tableName: Seq[String]): (String, String) = {
    if (tableName.size > 1) (tableName.head, tableName(1)) else if (tableName.size == 1) ("", tableName.head) else ("", "")
  }

  /**
   * 用于解析DDL语句中的库表、分区信息
   */
  def ddlParser(logicalPlan: LogicalPlan, sql: String = ""): Unit = {

  }

  /**
   * SQL语法校验
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  def sqlLegal(sql: String): Boolean = SparkUtils.sqlLegal(sql)
}
