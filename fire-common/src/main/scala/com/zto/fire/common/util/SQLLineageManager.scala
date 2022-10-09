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

import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.bean.lineage.{SQLTableColumns, _}
import com.zto.fire.predef._

/**
 * SQL血缘解析管理器，协助快速构建SQL血缘信息
 *
 * @author ChengLong 2022-09-01 15:10:38
 * @since 2.2.3
 */
private[fire] object SQLLineageManager {
  private lazy val statementSet = new JHashSet[String]()
  private lazy val relationSet = new JHashSet[SQLTableRelations]()
  private lazy val tableLineageMap = new JConcurrentHashMap[String, SQLTable]()

  /**
   * 添加待执行的SQL语句
   */
  def addStatement(statement: String): Unit = {
    if (noEmpty(statement)) this.statementSet.add(statement.trim)
  }

  /**
   * 维护表与表之间的关系
   *
   * @param srcTable
   * 数据来源表
   * @param sinkTable
   * 目标表
   */
  def addRelation(srcTableIdentifier: TableIdentifier, sinkTableIdentifier: TableIdentifier): Unit = {
    this.relationSet.add(new SQLTableRelations(srcTableIdentifier.toString, sinkTableIdentifier.toString))
  }

  /**
   * 获取SQL血缘信息
   */
  def getSQLLineage: SQLLineage = {
    val sqlLineage = new SQLLineage()
    sqlLineage.setStatements(this.statementSet.toList)
    sqlLineage.setTables(this.tableLineageMap.values().toList)
    sqlLineage.setRelations(this.relationSet.toList)
    sqlLineage
  }

  /**
   * 根据给定的库表名称获取完整表名
   *
   * @param dbName
   * 数据库名称（可为空）
   * @param tableName
   * 表名
   * @return
   * dbName.tableName
   */
  def getTableIdentify(tableIdentifier: TableIdentifier): String = {
    requireNonEmpty(tableIdentifier.table, "表名不能为空")
    tableIdentifier.toString
  }

  /**
   * 根据库表信息获取SQLTable实例
   *
   * @param dbName
   * 数据库名称，可为空
   * @param tableName
   * 表名
   * @return
   * SQLTable
   */
  def getTableInstance(tableIdentifier: TableIdentifier): SQLTable = {
    this.tableLineageMap.mergeGet(getTableIdentify(tableIdentifier)) {
      new SQLTable(tableIdentifier.toString)
    }
  }

  /**
   * 用于为指定的SQLTable对象添加必要的字段值
   */
  private[this] def setTableField(tableIdentifier: TableIdentifier)(fun: SQLTable => Unit): SQLTable = {
    val table = this.getTableInstance(tableIdentifier)
    fun(table)
    table
  }

  /**
   * 为指定的表添加options信息
   *
   * @param options
   * 选项信息
   */
  def setOptions(tableIdentifier: TableIdentifier, options: Map[String, String]): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.getOptions.putAll(options)
    }
  }

  /**
   * 为指定的表添加操作信息
   *
   * @param operations
   * 操作类型信息（INSERT、DROP等）
   */
  def setOperation(tableIdentifier: TableIdentifier, operations: String*): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.getOperation.addAll(operations)
    }
  }

  /**
   * 为指定的表添加使用到的字段信息
   *
   * @param columns
   * 字段列表
   */
  def setColumns(tableIdentifier: TableIdentifier, columns: Seq[(String, String)]): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.getColumns.addAll(columns.map(t => new SQLTableColumns(t._1, t._2)))
    }
  }

  /**
   * 为指定的表添加使用到的分区信息
   *
   * @param partitions
   * 分区列表
   */
  def setPartitions(tableIdentifier: TableIdentifier, partitions: Seq[(String, String)]): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.getPartitions.addAll(partitions.map(t => new SQLTablePartitions(t._1, t._2)))
    }
  }

  /**
   * 为指定的表添加catalog信息
   *
   * @param catalog
   * catalog信息：hive、kafka、jdbc等
   */
  def setCatalog(tableIdentifier: TableIdentifier, catalog: String): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.setCatalog(catalog)
    }
  }

  /**
   * 为指定的表添加comment信息
   *
   * @param comment
   * 表注释信息
   */
  def setComment(tableIdentifier: TableIdentifier, comment: String): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.setComment(comment)
    }
  }

  /**
   * 为指定的表添加catalog的集群url
   *
   * @param cluster
   * 集群地址
   */
  def setCluster(tableIdentifier: TableIdentifier, cluster: String): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.setCluster(cluster)
    }
  }

  /**
   * 为指定的表添加catalog的具体物理表名
   *
   * @param physicalTable
   * 真实的表名
   */
  def setPhysicalTable(tableIdentifier: TableIdentifier, physicalTable: String): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.setPhysicalTable(physicalTable)
    }
  }

  /**
   * 为指定的表添加视图名称
   *
   * @param tmpView
   * spark或flink任务内部注册的临时表名
   */
  def setTmpView(tableIdentifier: TableIdentifier, tmpView: String): SQLTable = {
    this.setTableField(tableIdentifier) {
      _.setTmpView(tmpView)
    }
  }

}
