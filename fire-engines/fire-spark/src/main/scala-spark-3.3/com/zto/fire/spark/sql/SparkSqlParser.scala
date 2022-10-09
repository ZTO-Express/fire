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

import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.enu.Operation
import com.zto.fire.common.util.SQLLineageManager
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.AlterTableRenamePartitionCommand
import org.apache.spark.sql.execution.datasources.CreateTable

/**
 * Spark SQL解析器，用于解析Spark SQL语句中的库、表、分区、操作类型等信息
 *
 * @author ChengLong 2021-6-18 16:31:04
 * @since 2.0.0
 */
private[fire] object SparkSqlParser extends SparkSqlParserBase {

  /**
   * 用于解析查询sql中的库表信息
   *
   * @param sinkTable
   * 当insert xxx select或create xxx select语句时，sinkTable不为空
   */
  override def queryParser(logicalPlan: LogicalPlan, sinkTable: Option[TableIdentifier]): Unit = {
    logicalPlan.children.foreach(child => {
      this.queryParser(child, sinkTable)
      var sourceTable: Option[TableIdentifier] = None
      child match {
        case unresolvedRelation: UnresolvedRelation =>
          this.addCatalog(unresolvedRelation.multipartIdentifier, Operation.SELECT)
          sourceTable = Some(toTableIdentifier(unresolvedRelation.multipartIdentifier))
          // 如果是insert xxx select或create xxx select语句，则维护表与表之间的关系
          if (sinkTable.isDefined) SQLLineageManager.addRelation(toTableIdentifier(unresolvedRelation.multipartIdentifier), sinkTable.get)
        case _ => this.logger.debug(s"Parse query SQL异常，无法匹配该Statement. ")
      }
    })
  }

  /**
   * 用于解析DDL语句中的库表、分区信息
   *
   * @return 返回sink目标表，用于维护表与表之间的关系
   */
  override def ddlParser(logicalPlan: LogicalPlan): Option[TableIdentifier] = {
    var sinkTable: Option[TableIdentifier] = None
    logicalPlan match {
      // insert into语句解析
      case insertInto: InsertIntoStatement => {
        val identifier = insertInto.table.asInstanceOf[UnresolvedRelation].multipartIdentifier
        this.addCatalog(identifier, Operation.INSERT_INTO)
        // 维护分区信息
        val fireTableIdentifier = toTableIdentifier(identifier)
        val partitions = insertInto.partitionSpec.map(part => (part._1, if (part._2.isDefined) part._2.get else ""))
        SQLLineageManager.setPartitions(fireTableIdentifier, partitions.toSeq)
        sinkTable = Some(fireTableIdentifier)
      }
      // rename table语句解析
      case renameTable: RenameTable => {
        this.addCatalog(renameTable.newName, Operation.RENAME_TABLE_OLD)
        this.addCatalog(renameTable.newName, Operation.RENAME_TABLE_NEW)
        SQLLineageManager.addRelation(toTableIdentifier(renameTable.newName), toTableIdentifier(renameTable.newName))
      }
      // create table as select语句解析
      case createTableAsSelect: CreateTableAsSelect => {
        val identifier = TableIdentifier(createTableAsSelect.tableName.name())
        this.addCatalog(identifier, Operation.CREATE_TABLE_AS_SELECT)
        // 采集建表属性信息
        SQLLineageManager.setOptions(identifier, createTableAsSelect.writeOptions)
        sinkTable = Some(identifier)
      }
      // create table语句解析
      case createTable: CreateTable => {
        val identifier = this.toFireTableIdentifier(createTable.tableDesc.identifier)
        this.addCatalog(identifier, Operation.CREATE_TABLE)
        sinkTable = Some(identifier)
        // 采集建表属性信息
        SQLLineageManager.setOptions(identifier, createTable.tableDesc.properties)
        // 采集分区字段信息
        val partitions = createTable.tableDesc.partitionSchema.map(st => (st.dataType.toString, st.name))
        SQLLineageManager.setPartitions(identifier, partitions)
      }
      // rename partition语句解析
      case renamePartition: AlterTableRenamePartitionCommand => {
        val table = this.toFireTableIdentifier(renamePartition.tableName)
        this.addCatalog(table, Operation.RENAME_PARTITION_OLD)
        this.addCatalog(table, Operation.RENAME_PARTITION_NEW)
        SQLLineageManager.setPartitions(table, renamePartition.oldPartition.toSeq)
        SQLLineageManager.setPartitions(table, renamePartition.newPartition.toSeq)
      }
      case _ => this.logger.debug(s"Parse ddl SQL异常，无法匹配该Statement.")
    }
    sinkTable
  }
}
