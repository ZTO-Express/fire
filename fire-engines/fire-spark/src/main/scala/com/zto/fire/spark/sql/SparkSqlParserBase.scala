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

import com.zto.fire._
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.util.SQLLineageManager
import com.zto.fire.core.sql.SqlParser
import com.zto.fire.predef.JConcurrentHashMap
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{TableIdentifier => SparkTableIdentifier}
import org.apache.spark.sql.execution.datasources.CreateTable


/**
 * Spark SQL解析器父类，封装各个spark版本通用的api
 *
 * @author ChengLong 2022-09-07 15:31:03
 * @since 2.3.2
 */
@Internal
private[fire] trait SparkSqlParserBase extends SqlParser {
  protected lazy val spark = SparkSingletonFactory.getSparkSession
  protected lazy val catalog = this.spark.sessionState.catalog
  protected lazy val hiveTableMetaDataMap = new JConcurrentHashMap[String, CatalogTable]()

  /**
   * 用于判断给定的表是否为hive表
   *
   * @param tableIdentifier 库表
   */
  @Internal
  protected def getCatalog(tableIdentifier: TableIdentifier): Datasource = {
    val isHive = this.isHiveTable(tableIdentifier)
    if (isHive) Datasource.HIVE else Datasource.VIEW
  }

  /**
   * 将Fire的TableIdentifier转为Spark的TableIdentifier
   */
  @Internal
  private[fire] def toSparkTableIdentifier(tableIdentifier: TableIdentifier): SparkTableIdentifier = {
    val db = if (isEmpty(tableIdentifier.database)) None else Some(tableIdentifier.database)
    SparkTableIdentifier(tableIdentifier.table, db)
  }

  /**
   * 将Spark的TableIdentifier转为Fire的TableIdentifier
   */
  @Internal
  private[fire] def toFireTableIdentifier(tableIdentifier: SparkTableIdentifier): TableIdentifier = {
    TableIdentifier(tableIdentifier.unquotedString)
  }

  /**
   * 用于判断表是否存在
   */
  @Internal
  private[fire] def tableExists(tableIdentifier: TableIdentifier): Boolean = {
    tryWithReturn {
      this.catalog.tableExists(toSparkTableIdentifier(tableIdentifier))
    } (this.logger, catchLog = s"判断${tableIdentifier}是否存在发生异常", hook = false)
  }

  /**
   * 用于判断给定的表是否为临时表
   */
  @Internal
  override def isTempView(tableIdentifier: TableIdentifier): Boolean = {
    tryWithReturn {
      catalog.isTemporaryTable(toSparkTableIdentifier(tableIdentifier))
    } (this.logger, catchLog = s"判断${tableIdentifier}是否为临时表或视图失败", hook = false)
  }

  /**
   * 用于判断给定的表是否为hive表
   */
  @Internal
  override def isHiveTable(tableIdentifier: TableIdentifier): Boolean = {
    this.hiveTableMap.mergeGet(tableIdentifier.identifier) {
      if (this.isTempView(tableIdentifier) || !this.tableExists(tableIdentifier)) return false
      tryWithReturn {
        val hiveTable = this.hiveTableMetaDataMap.mergeGet(tableIdentifier.identifier) {
          catalog.getTableMetadata(toSparkTableIdentifier(tableIdentifier))
        }
        if (hiveTable.provider.isDefined && "hive".equals(hiveTable.provider.get)) true else false
      } (this.logger, catchLog = s"判断${tableIdentifier}是否为hive表失败", hook = false)
    }
  }

  /**
   * 将解析到的表信息添加到实时血缘中
   */
  @Internal
  protected def addCatalog(identifierSeq: Seq[String], operation: Operation): Unit = {
    val identifier = this.toTableIdentifier(identifierSeq)
    this.addCatalog(identifier, operation)
  }

  /**
   * 将解析到的表信息添加到实时血缘中
   */
  @Internal
  protected def addCatalog(identifier: TableIdentifier, operation: Operation): Unit = {
    SQLLineageManager.setCatalog(identifier, this.getCatalog(identifier).toString)
    SQLLineageManager.setOperation(identifier, operation.toString)
    if (this.isTempView(identifier)) {
      SQLLineageManager.setTmpView(identifier, identifier.toString())
    }
    if (this.isHiveTable(identifier)) {
      val metadata = this.hiveTableMetaDataMap.get(identifier.toString)
      if (metadata != null) {
        val url = metadata.storage.locationUri
        if (url.isDefined) SQLLineageManager.setCluster(identifier, url.get.toString)
        // 添加表属性信息
        SQLLineageManager.setOptions(identifier, metadata.properties)
        // 添加字段信息
        val columns = metadata.schema.map(field => (field.name, field.dataType.toString))
        if (columns.nonEmpty) SQLLineageManager.setColumns(identifier, columns)
        // 表注释信息
        if (metadata.comment.isDefined) SQLLineageManager.setComment(identifier, metadata.comment.get)
      }
    }
  }

  /**
   * 获取库表名
   *
   * @param tableName 解析后的表信息
   */
  @Internal
  protected def toTableIdentifier(tableName: Seq[String]): TableIdentifier = {
    if (tableName.size > 1)
      TableIdentifier(tableName(1), tableName.head)
    else if (tableName.size == 1) TableIdentifier(tableName.head)
    else TableIdentifier("")
  }

  /**
   * SQL语法校验
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  def sqlLegal(sql: String): Boolean = SparkUtils.sqlLegal(sql)

  /**
   * 用于解析SparkSql中的库表信息
   */
  @Internal
  override def sqlParser(sql: String): Unit = {
    if (isEmpty(sql)) return
    tryWithLog {
      this.logger.debug(s"开始解析sql语句：$sql")
      SparkUtils.sqlValidate(sql)
      val logicalPlan = this.spark.sessionState.sqlParser.parsePlan(sql)
      SQLLineageManager.addStatement(sql)
      val sinkTable = this.ddlParser(logicalPlan)
      this.queryParser(logicalPlan, sinkTable)
    } (this.logger, catchLog = s"可忽略异常：实时血缘解析SQL报错，SQL：\n$sql", hook = false)
  }

  /**
   * 用于解析查询sql中的库表信息
   *
   * @param sinkTable
   * 当insert xxx select或create xxx select语句时，sinkTable不为空
   */
  @Internal
  protected def queryParser(logicalPlan: LogicalPlan, sinkTable: Option[TableIdentifier]): Unit

  /**
   * 用于解析DDL语句中的库表、分区信息
   *
   * @return 返回sink目标表，用于维护表与表之间的关系
   */
  @Internal
  protected def ddlParser(logicalPlan: LogicalPlan): Option[TableIdentifier]
}
