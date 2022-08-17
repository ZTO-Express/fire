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
  private var throwCount = 0

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
  override def isHiveTable(dbName: String = null, tableName: String): Boolean = {
    val tableIdentifier = this.tableIdentifier(dbName, tableName)

    if (!this.hiveTableMap.contains(tableIdentifier)) {
      // 根据catalog判断是否为临时表
      if (this.isTempView(dbName, tableName)) {
        this.hiveTableMap.put(tableIdentifier, false)
      } else {
        try {
          // 非临时表，进行重量级的解析，依据storage的存储路径进行判断是否为hive表
          val catalog = this.spark.sessionState.catalog.getTablesByName(Seq(new TableIdentifier(tableName, if (isEmpty(dbName)) Some("default") else Some(dbName))))
          if (catalog.isEmpty) {
            this.hiveTableMap.put(tableIdentifier, false)
          } else {
            val isHiveTable = catalog.head.storage.locationUri.getOrElse("").toString.contains("hdfs")
            this.hiveTableMap.put(tableIdentifier, isHiveTable)
          }
        } catch {
          case e: Exception => {
            this.throwCount += 1
            if (throwCount <= 3) this.logger.warn(s"可忽略异常：确认${dbName}.${tableName}是否为hive表失败", e)
          }
        }
      }
    }

    this.hiveTableMap.getOrDefault(tableIdentifier, false)
  }

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
    tryWithLog {
      logicalPlan.children.foreach(child => {
        this.sqlQueryParser(child, sql)
        child match {
          case unresolvedRelation: UnresolvedRelation =>
            val identifier = unresolvedRelation.multipartIdentifier
            val dbTable = this.tableName(identifier)
            this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), TableMeta(dbTable._1, dbTable._2, datasource = this.isHiveTableDatasource(dbTable), operation = Operation.SELECT))
          case _ => this.logger.debug(s"Parse query SQL异常，无法匹配该Statement. sql->$sql")
        }
      })
    }(this.logger, isThrow = false)
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
    tryWithLog {
      logicalPlan match {
        // insert into语句解析
        case insertInto: InsertIntoStatement => {
          val identifier = insertInto.table.asInstanceOf[UnresolvedRelation].multipartIdentifier
          val dbTable = this.tableName(identifier)
          val table = TableMeta(dbTable._1, dbTable._2, mutable.Map(insertInto.partitionSpec.map(t => (t._1, t._2.getOrElse(""))).toSeq: _*), this.isHiveTableDatasource(dbTable), Operation.INSERT_INTO)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        // drop table语句解析
        case dropTable: DropTableStatement => {
          val dbTable = this.tableName(dropTable.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, datasource = this.isHiveTableDatasource(dbTable), operation = Operation.DROP_TABLE)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        // rename table语句解析
        case renameTable: RenameTableStatement => {
          val oldDBTableTuple = this.tableName(renameTable.oldName)
          val oldTable = TableMeta(oldDBTableTuple._1, oldDBTableTuple._2, datasource = this.isHiveTableDatasource(oldDBTableTuple), operation = Operation.RENAME_TABLE_OLD)
          this.addTmpTableMeta(this.tableIdentifier(oldDBTableTuple._1, oldDBTableTuple._2), oldTable)

          val newDBTableTuple = this.tableName(renameTable.newName)
          val newTable = TableMeta(newDBTableTuple._1, newDBTableTuple._2, datasource = this.isHiveTableDatasource(newDBTableTuple), operation = Operation.RENAME_TABLE_NEW)
          this.addTmpTableMeta(this.tableIdentifier(newDBTableTuple._1, newDBTableTuple._2), newTable)
        }
        // create table语句解析
        case createTable: CreateTable => {
          val table = createTable.tableDesc.identifier.table.split('.')
          val dbTable = this.tableName(table)
          val map = mutable.Map[String, String]()
          createTable.tableDesc.partitionColumnNames.foreach(partition => map.put(partition, ""))
          val tableObj = TableMeta(dbTable._1, dbTable._2, map, this.isHiveTableDatasource(dbTable), Operation.CREATE_TABLE)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), tableObj)
        }
        // create table as select语句解析
        case createTableAsSelect: CreateTableAsSelectStatement => {
          val dbTable = this.tableName(createTableAsSelect.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, datasource = this.isHiveTableDatasource(dbTable), operation = Operation.CREATE_TABLE_AS_SELECT)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        // rename partition语句解析
        case renamePartition: AlterTableRenamePartitionStatement => {
          val dbTable = this.tableName(renamePartition.tableName)
          val catalog = this.isHiveTableDatasource(dbTable)
          val oldTable = TableMeta(dbTable._1, dbTable._2, mutable.Map(renamePartition.from.toSeq: _*), catalog, Operation.ALTER_TABLE_RENAME_PARTITION_OLD)
          val newTable = TableMeta(dbTable._1, dbTable._2, mutable.Map(renamePartition.to.toSeq: _*), catalog, Operation.ALTER_TABLE_RENAME_PARTITION_NEW)
          this.addTmpTableMeta(this.tableIdentifier("old_" + dbTable._1, dbTable._2), oldTable)
          this.addTmpTableMeta(this.tableIdentifier("new_" + dbTable._1, dbTable._2), newTable)
        }
        // drop partition语句解析
        case dropPartition: AlterTableDropPartitionStatement => {
          val dbTable = this.tableName(dropPartition.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, mutable.Map(dropPartition.specs.head.toSeq: _*), this.isHiveTableDatasource(dbTable), Operation.ALTER_TABLE_DROP_PARTITION)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        // add partition语句解析
        case addPartition: AlterTableAddPartitionStatement => {
          val dbTable = this.tableName(addPartition.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, mutable.Map(addPartition.partitionSpecsAndLocs.head._1.toSeq: _*), this.isHiveTableDatasource(dbTable), Operation.ALTER_TABLE_ADD_PARTITION)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        // truncate table语句解析
        case truncateTable: TruncateTableStatement => {
          val dbTable = this.tableName(truncateTable.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, datasource = this.isHiveTableDatasource(dbTable), operation = Operation.TRUNCATE)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        case cacheTable: CacheTableStatement => {
          val dbTable = this.tableName(cacheTable.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, datasource = this.isHiveTableDatasource(dbTable), operation = Operation.CACHE)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        case uncacheTable: UncacheTableStatement => {
          val dbTable = this.tableName(uncacheTable.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, datasource = this.isHiveTableDatasource(dbTable), operation = Operation.UNCACHE)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        case refreshTable: RefreshTableStatement => {
          val dbTable = this.tableName(refreshTable.tableName)
          val table = TableMeta(dbTable._1, dbTable._2, datasource = this.isHiveTableDatasource(dbTable), operation = Operation.REFRESH)
          this.addTmpTableMeta(this.tableIdentifier(dbTable._1, dbTable._2), table)
        }
        case createDatabase: CreateNamespaceStatement => {
          val table = TableMeta(createDatabase.namespace.head, "", datasource = Datasource.HIVE, operation = Operation.CREATE_DATABASE)
          this.addTmpTableMeta(createDatabase.namespace.head, table)
        }
        case dropDatabase: DropNamespace => {
          val dbName = dropDatabase.namespace.asInstanceOf[UnresolvedNamespace].multipartIdentifier.head
          val table = TableMeta(dbName, datasource = Datasource.HIVE, operation = Operation.DROP_DATABASE)
          this.addTmpTableMeta(dbName, table)
        }
        case _ => this.logger.debug(s"Parse ddl SQL异常，无法匹配该Statement. sql->$sql")
      }
    }(this.logger, isThrow = false)
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
