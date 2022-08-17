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

package com.zto.fire.flink.sql

import com.zto.fire._
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.util.{ExceptionBus, TableMeta}
import com.zto.fire.core.sql.SqlParser
import com.zto.fire.flink.util.{FlinkSingletonFactory, FlinkUtils}
import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.{SqlParser => CalciteParser}
import org.apache.flink.sql.parser.SqlProperty
import org.apache.flink.sql.parser.ddl._
import org.apache.flink.sql.parser.dml._
import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.ObjectPath

import scala.collection.JavaConversions
import scala.util.Try

/**
 * Flink SQL解析器，用于解析Flink SQL语句中的库、表、分区、操作类型等信息
 *
 * @author ChengLong 2021-6-18 16:41:04
 * @since 2.0.0
 */
object FlinkSqlParser extends SqlParser {
  // calcite parser config
  private lazy val config = createParserConfig
  private lazy val hiveConfig = createHiveParserConfig
  private lazy val tableEnv = FlinkSingletonFactory.getTableEnv.asInstanceOf[StreamTableEnvironment]

  /**
   * 构建flink default的SqlParser config
   */
  @Internal
  def createParserConfig: CalciteParser.Config = {
    CalciteParser.configBuilder.setParserFactory(
      FlinkSqlParserImpl.FACTORY).
      setQuoting(Quoting.BACK_TICK).
      setUnquotedCasing(Casing.TO_UPPER).
      setQuotedCasing(Casing.UNCHANGED).
      // setConformance(FlinkSqlConformance.DEFAULT).
      build
  }

  /**
   * 构建flink hive方言版的SqlParser config
   */
  @Internal
  def createHiveParserConfig: CalciteParser.Config = {
    CalciteParser.configBuilder.setParserFactory(
      FlinkHiveSqlParserImpl.FACTORY).
      setQuoting(Quoting.BACK_TICK).
      setUnquotedCasing(Casing.TO_UPPER).
      setQuotedCasing(Casing.UNCHANGED).
      // setConformance(FlinkSqlConformance.DEFAULT).
      build
  }

  /**
   * 根据sql构建Calcite SqlParser
   */
  def parser(sql: String, config: CalciteParser.Config = this.config): SqlNode = {
    CalciteParser.create(sql, config).parseStmt()
  }

  /**
   * 用户解析库表信息以及数据源类型
   */
  @Internal
  private def setTableName(seq: Seq[String], operation: Operation): Unit = {
    // 获取数据源类型（根据with参数列表中的connector）
    val datasource = operation match {
      case Operation.CREATE_VIEW => Datasource.VIEW
      case Operation.CREATE_TABLE => if (this.tableEnv.isHiveCatalog) Datasource.HIVE else Datasource.VIEW
      case Operation.CREATE_TABLE_AS_SELECT => if (this.tableEnv.isHiveCatalog) Datasource.HIVE else Datasource.VIEW
      case Operation.CREATE_DATABASE => if (this.tableEnv.isHiveCatalog) Datasource.HIVE else Datasource.VIEW
      case Operation.DROP_TABLE => if (this.tableEnv.isHiveCatalog) Datasource.HIVE else Datasource.VIEW
      case Operation.DROP_DATABASE => if (this.tableEnv.isHiveCatalog) Datasource.HIVE else Datasource.VIEW
      case _ => {
        if (seq.size == 1) {
          if (this.isHiveTable(null, seq.head)) Datasource.HIVE else Datasource.VIEW
        } else {
          if (this.isHiveTable(seq.head, seq(1))) Datasource.HIVE else Datasource.VIEW
        }
      }
    }

    if (seq.size == 1) {
      val table = TableMeta("", seq.head, datasource = datasource, operation = operation)
      this.addTmpTableMeta(this.tableIdentifier("", seq.head), table)
    } else {
      val table = TableMeta(seq.head, seq(1), datasource = datasource, operation = operation)
      this.addTmpTableMeta(this.tableIdentifier(seq.head, seq(1)), table)
    }
  }

  /**
   * 获取库表信息，以逗号分隔
   */
  @Internal
  private def getTableIdentifier(sqlIdentifier: SqlIdentifier): String = {
    val seq = sqlIdentifier.names.toSeq
    if (seq.size == 1) {
      this.tableIdentifier("", seq.head)
    } else {
      this.tableIdentifier(seq.head, seq(1))
    }
  }

  /**
   * 解析SQL中的分区信息
   */
  @Internal
  def getPartitions(sqlIdentifier: SqlIdentifier, partitionsNode: Seq[SqlNodeList]): Unit = {
    val tableIdentifier = this.getTableIdentifier(sqlIdentifier)
    val partitions = partitionsNode.flatMap(sqlNodeList => sqlNodeList.getList.map(sqlNode => sqlNode.asInstanceOf[SqlProperty])).map(partitionNode => partitionNode.getKeyString -> partitionNode.getValueString).toMap
    val table = this.tmpTableMap.get(tableIdentifier)
    if (table != null) {
      table.partition ++= partitions
      this.addTmpTableMeta(tableIdentifier, table)
    }
  }

  /**
   * 解析查询SQL中的SqlNode
   */
  @Internal
  private[this] def parseSqlNode(sqlNode: SqlNode, operation: Operation = Operation.SELECT): Unit = {
    sqlNode match {
      case select: SqlSelect => this.parseSqlNode(select.getFrom)
      case sqlJoin: SqlJoin => {
        this.parseSqlNode(sqlJoin.getLeft, operation)
        this.parseSqlNode(sqlJoin.getRight, operation)
      }
      case sqlBasicCall: SqlBasicCall => this.parseSqlNode(sqlBasicCall.operands(0))
      case sqlIdentifier: SqlIdentifier => {
        this.setTableName(sqlIdentifier.names.toSeq, operation)
      }
      case sqlNodeList: SqlNodeList => JavaConversions.asScalaBuffer(sqlNodeList.getList).foreach(this.parseSqlNode(_))
      // 解析分区信息
      case sqlProperty: SqlProperty => {
        this.logger.info(s"解析SQL分区信息：partition=${sqlProperty.getKeyString} value=${sqlProperty.getValueString}")
      }
      // 解析with列表中的属性
      case sqlTableOption: SqlTableOption => {
        this.logger.info(s"解析SQL with参数信息：partition=${sqlTableOption.getKeyString} value=${sqlTableOption.getValueString}")
      }
      case sqlTableLike: SqlTableLike => this.parseSqlNode(sqlTableLike.getSourceTable)
      case _ => this.logger.warn("不支持的SqlNode")
    }
  }

  /**
   * 用于解析给定的SQL语句
   */
  override def sqlParser(sql: String): Unit = {
    try {
      this.parser(sql) match {
        case select: SqlSelect => this.parseSqlNode(select)
        case insert: RichSqlInsert => {
          this.parseSqlNode(insert.getTargetTable, Operation.INSERT_INTO)
          this.getPartitions(insert.getTargetTable.asInstanceOf[SqlIdentifier], Seq(insert.getStaticPartitions))
          // this.parseSqlNode(insert.getStaticPartitions, Operation.INSERT_INTO)
          this.parseSqlNode(insert.getSource, Operation.SELECT)
        }
        case createView: SqlCreateView => {
          this.parseSqlNode(createView.getViewName, Operation.CREATE_VIEW)
          this.parseSqlNode(createView.getQuery, Operation.SELECT)
        }
        case createTable: SqlCreateTable => {
          this.parseSqlNode(createTable.getTableName, Operation.CREATE_TABLE)
          val tableLike = createTable.getTableLike
          if (tableLike.isPresent) {
            // create table like语句
            this.parseSqlNode(tableLike.get(), Operation.SELECT)
          } else {
            // create table语句
            val tableIdentifier = this.getTableIdentifier(createTable.getTableName)
            // 解析建表语句中的with参数列表
            val properties = createTable.getPropertyList.map(prop => {
              val sqlProp = prop.asInstanceOf[SqlTableOption]
              (sqlProp.getKeyString, sqlProp.getValueString)
            }).toMap

            // 绑定with参数与表对象
            val table = this.tmpTableMap.get(tableIdentifier)
            if (table != null) {
              table.properties ++= properties
              val connector = properties.get("connector")
              if (connector.nonEmpty) table.datasource = Datasource.parse(connector.get)
              this.addTmpTableMeta(tableIdentifier, table)
            }
          }
        }
        case _ => this.hiveSqlParser(sql)
      }
    } catch {
      case _: Throwable => this.hiveSqlParser(sql)
    }
  }

  /**
   * 用于解析Hive SQL
   */
  def hiveSqlParser(sql: String): Unit = {
    this.parser(sql, this.hiveConfig) match {
      case sqlAddPartitions: SqlAddPartitions => {
        this.parseSqlNode(sqlAddPartitions.getTableName, Operation.ALTER_TABLE_ADD_PARTITION)
        this.getPartitions(sqlAddPartitions.getTableName, sqlAddPartitions.getPartSpecs)
      }
      case sqlCreateDatabase: SqlCreateDatabase => {
        this.parseSqlNode(sqlCreateDatabase.getDatabaseName, Operation.CREATE_DATABASE)
      }
      case sqlDropDatabase: SqlDropDatabase => {
        this.parseSqlNode(sqlDropDatabase.getDatabaseName, Operation.DROP_DATABASE)
      }
      case sqlDropPartitions: SqlDropPartitions => {
        this.parseSqlNode(sqlDropPartitions.getTableName, Operation.ALTER_TABLE_DROP_PARTITION)
        this.getPartitions(sqlDropPartitions.getTableName, sqlDropPartitions.getPartSpecs)
      }
      case sqlDropTable: SqlDropTable => {
        this.parseSqlNode(sqlDropTable.getTableName, Operation.DROP_TABLE)
      }
      case sqlAlterTableRename: SqlAlterTableRename => {
        // rename 后的新表明无法获取到，源码中使用private进行修饰（可反射获取）
        this.parseSqlNode(sqlAlterTableRename.getTableName, Operation.RENAME_TABLE_OLD)
      }
      case sqlAlterTable: SqlAlterTable => {
        this.parseSqlNode(sqlAlterTable.getTableName, Operation.ALTER_TABLE)
      }
      case _ => this.logger.debug(s"可忽略异常：实时血缘解析SQL报错，SQL：\n${sql}")
    }
  }

  /**
   * 用于判断给定的表是否为临时表
   */
  override def isTempView(dbName: String, tableName: String): Boolean = {
    try {
      if (this.tableEnv.defaultCatalog.isPresent) {
        val catalog = this.tableEnv.defaultCatalog.get()
        val db = if (isEmpty(dbName)) catalog.getDefaultDatabase else dbName
        catalog.tableExists(new ObjectPath(db, tableName))
      } else {
        false
      }
    } catch {
      case e => {
        this.logger.error(s"可忽略异常：判断tmp view失败, db name is ${dbName}, table name is ${tableName}", e)
        false
      }
    }
  }

  /**
   * 用于判断给定的表是否为hive表
   */
  override def isHiveTable(dbName: String, tableName: String): Boolean = {
    val tableIdentifier = s"$dbName.$tableName"

    if (!this.hiveTableMap.contains(tableIdentifier)) {
      // 根据catalog判断是否为临时表
      if (this.isTempView(dbName, tableName)) {
        this.hiveTableMap.put(tableIdentifier, false)
      } else {
        // 非临时表基于hive catalog进行判断
        if (!this.tableEnv.hiveCatalog.isPresent) {
          this.hiveTableMap.put(tableIdentifier, false)
        } else {
          val catalog = this.tableEnv.hiveCatalog.get()
          val db = if (isEmpty(dbName)) catalog.getDefaultDatabase else dbName
          try {
            if (catalog.tableExists(new ObjectPath(db, tableName))) {
              this.hiveTableMap.put(tableIdentifier, true)
            } else {
              this.hiveTableMap.put(tableIdentifier, false)
            }
          }
        }
      }
    }

    this.hiveTableMap(tableIdentifier)
  }

  /**
   * SQL语法校验
   *
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  override def sqlLegal(sql: JString): Boolean = FlinkUtils.sqlLegal(sql)
}
