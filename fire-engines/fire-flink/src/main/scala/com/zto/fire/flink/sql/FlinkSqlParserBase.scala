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
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.conf.{FireHiveConf, FireKafkaConf, FireRocketMQConf}
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.util.{LineageManager, ReflectionUtils, SQLLineageManager}
import com.zto.fire.core.sql.SqlParser
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.util.{FlinkSingletonFactory, FlinkUtils}
import com.zto.fire.jdbc.conf.FireJdbcConf
import org.apache.calcite.sql._
import org.apache.flink.sql.parser.SqlProperty
import org.apache.flink.sql.parser.ddl._
import org.apache.flink.sql.parser.dml._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.hadoop.hive.metastore.api.Table

import scala.collection.JavaConversions

/**
 * Flink SQL解析器，用于解析Flink SQL语句中的库、表、分区、操作类型等信息
 *
 * @author ChengLong 2021-6-18 16:41:04
 * @since 2.0.0
 */
@Internal
private[fire] trait FlinkSqlParserBase extends SqlParser {
  // calcite parser config
  protected lazy val tableEnv = FlinkSingletonFactory.getTableEnv.asInstanceOf[StreamTableEnvironment]
  protected lazy val hiveTableMetaDataMap = new JConcurrentHashMap[String, Table]()

  /**
   * 用于解析给定的SQL语句
   */
  override def sqlParser(sql: String): Unit = {
    try {
      FlinkUtils.sqlParser(sql) match {
        case select: SqlSelect => this.parseSqlNode(select)
        case insert: RichSqlInsert => {
          this.parseSqlNode(insert.getTargetTable, Operation.INSERT_INTO)
          this.parsePartitions(insert.getTargetTable.asInstanceOf[SqlIdentifier], Seq(insert.getStaticPartitions))
          this.parseSqlNode(insert.getSource, Operation.SELECT, targetTable = Some(insert.getTargetTable))
        }
        case createView: SqlCreateView => {
          this.parseSqlNode(createView.getViewName, Operation.CREATE_VIEW)
          this.parseSqlNode(createView.getQuery, Operation.SELECT)
        }
        case createTable: SqlCreateTable => parseCreateTable(createTable)
        case _ => this.hiveSqlParser(sql)
      }
    } catch {
      case e: Throwable => this.hiveSqlParser(sql)
    }
  }

  /**
   * 用于解析Hive SQL
   */
  @Internal
  protected def hiveSqlParser(sql: String): Unit = {
    FlinkUtils.sqlParser(sql, FlinkUtils.calciteHiveParserConfig) match {
      case sqlAddPartitions: SqlAddPartitions => {
        this.parseSqlNode(sqlAddPartitions.getTableName, Operation.ADD_PARTITION, true)
        this.parsePartitions(sqlAddPartitions.getTableName, sqlAddPartitions.getPartSpecs)
      }
      case sqlDropPartitions: SqlDropPartitions => {
        this.parseSqlNode(sqlDropPartitions.getTableName, Operation.DROP_PARTITION, true)
        this.parsePartitions(sqlDropPartitions.getTableName, sqlDropPartitions.getPartSpecs)
      }
      case sqlDropTable: SqlDropTable => this.parseSqlNode(sqlDropTable.getTableName, Operation.DROP_TABLE, true)
      case sqlDropDatabase: SqlDropDatabase => this.parseSqlNode(sqlDropDatabase.getDatabaseName, Operation.DROP_DATABASE)
      case sqlAlterTable: SqlAlterTable => this.parseSqlNode(sqlAlterTable.getTableName, Operation.ALTER_TABLE, true)
      case sqlCreateDatabase: SqlCreateDatabase => this.parseSqlNode(sqlCreateDatabase.getDatabaseName, Operation.CREATE_DATABASE, true)
      case sqlAlterTableRename: SqlAlterTableRename => this.parseSqlNode(sqlAlterTableRename.getTableName, Operation.RENAME_TABLE_OLD, true)
      case sqlCreateTable: SqlCreateTable => this.parseHiveCreateTable(sqlCreateTable)
      case _ => this.logger.info(s"可忽略异常：实时血缘解析SQL报错，SQL：\n$sql")
    }
  }

  /**
   * 解析查询SQL中的SqlNode
   */
  @Internal
  protected def parseSqlNode(sqlNode: SqlNode, operation: Operation = Operation.SELECT, isHive: Boolean = false, targetTable: Option[SqlNode] = None): Unit = {
    sqlNode match {
      case select: SqlSelect => this.parseSqlNode(select.getFrom, operation, isHive, targetTable)
      case sqlJoin: SqlJoin => {
        this.parseSqlNode(sqlJoin.getLeft, operation, isHive, targetTable)
        this.parseSqlNode(sqlJoin.getRight, operation, isHive, targetTable)
      }
      case sqlBasicCall: SqlBasicCall => {
        sqlBasicCall.operands.foreach(sqlNode => {
          // 过滤掉别名
          if (sqlNode.isInstanceOf[SqlIdentifier]) {
            val sqlIdentifier = sqlNode.asInstanceOf[SqlIdentifier]
            val componentPositions = ReflectionUtils.getFieldByName(sqlIdentifier.getClass, "componentPositions")
            if (componentPositions.get(sqlIdentifier) == null) return
          }
          if (sqlNode.isInstanceOf[SqlSnapshot]) {
            this.parseSqlNode(sqlNode.asInstanceOf[SqlSnapshot].getTableRef, operation, isHive, targetTable)
          }
          this.parseSqlNode(sqlNode, operation, isHive, targetTable)
        })
      }
      case sqlIdentifier: SqlIdentifier => {
        val tableIdentifier = toFireTableIdentifier(sqlIdentifier, isHive)
        this.addCatalog(tableIdentifier, operation)
        if (targetTable.isDefined) {
          SQLLineageManager.addRelation(tableIdentifier, TableIdentifier(targetTable.get.toString))
        }
      }
      case sqlNodeList: SqlNodeList => JavaConversions.asScalaBuffer(sqlNodeList.getList).foreach(this.parseSqlNode(_))
      case sqlTableLike: SqlTableLike => this.parseSqlNode(sqlTableLike.getSourceTable, operation, isHive, targetTable)
      case _ =>
    }
  }


  /**
   * 移除表的catalog名称
   */
  protected def replaceCatalogName(tableName: String): String = {
    tableName.replace(FireHiveConf.hiveCatalogName + ".", "").replace(FireFlinkConf.defaultCatalogName + ".", "")
  }

  /**
   * 将Fire的TableIdentifier转为Flink的ObjectPath对象
   *
   * @param isHiveTable
   * 如果是hive表，则默认的数据库名称从配置文件中获取，否则从env中获取默认的数据库名称
   */
  @Internal
  protected def toFlinkTableIdentifier(tableIdentifier: TableIdentifier, isHiveTable: Boolean = false): ObjectPath = {
    val db = if (noEmpty(tableIdentifier.database)) tableIdentifier.database else if (isHiveTable) FireHiveConf.defaultDB else this.tableEnv.defaultCatalog.get().getDefaultDatabase
    new ObjectPath(db, tableIdentifier.table)
  }


  /**
   * 将Flink的ObjectPath对象转为Fire的TableIdentifier
   */
  @Internal
  protected def toFireTableIdentifier(objectPath: ObjectPath): TableIdentifier = {
    val db = if (noEmpty(objectPath.getDatabaseName)) objectPath.getDatabaseName else this.tableEnv.defaultCatalog.get().getDefaultDatabase
    TableIdentifier(db, objectPath.getObjectName)
  }

  /**
   * 将Flink的SqlIdentifier转为Fire的TableIdentifier
   */
  @Internal
  protected def toFireTableIdentifier(sqlIdentifier: SqlIdentifier, isHive: Boolean): TableIdentifier = {
    val tableName = this.replaceCatalogName(sqlIdentifier.toString.toLowerCase)
    if (isHive) this.toFireHiveTableIdentifier(TableIdentifier(tableName)) else TableIdentifier(tableName)
  }

  /**
   * 补全hive表所在的数据库信息
   */
  @Internal
  protected def toFireHiveTableIdentifier(tableIdentifier: TableIdentifier): TableIdentifier = {
    val db = if (tableIdentifier.notExistsDB) FireHiveConf.defaultDB else tableIdentifier.database
    TableIdentifier(tableIdentifier.table, db)
  }

  /**
   * 用于判断给定的表是否为临时表
   */
  override def isTempView(tableIdentifier: TableIdentifier): Boolean = {
    if (this.tableEnv.defaultCatalog.isPresent) {
      val catalog = this.tableEnv.defaultCatalog.get()
      catalog.tableExists(this.toFlinkTableIdentifier(tableIdentifier))
    } else {
      false
    }
  }

  /**
   * 获取Hive表元数据信息
   */
  @Internal
  protected def getHiveTable(tableIdentifier: TableIdentifier): Option[Table] = {
    if (!this.tableEnv.hiveCatalog.isPresent) return None
    // 获取hive表所在的数据库名称
    val hiveTableIdentifier = if (tableIdentifier.notExistsDB) TableIdentifier(tableIdentifier.table, FireHiveConf.defaultDB) else tableIdentifier
    val hiveTable = this.hiveTableMetaDataMap.mergeGet(hiveTableIdentifier.identifier) {
      this.tableEnv.hiveCatalog.get().asInstanceOf[HiveCatalog].getHiveTable(this.toFlinkTableIdentifier(hiveTableIdentifier, true))
    }
    Some(hiveTable)
  }

  /**
   * 用于判断给定的表是否为hive表
   */
  @Internal
  override def isHiveTable(tableIdentifier: TableIdentifier): Boolean = {
    this.hiveTableMap.mergeGet(tableIdentifier.identifier) {
      tryWithReturn {
        if (this.tableEnv.hiveCatalog.isPresent) {
          val hiveCatalog = this.tableEnv.hiveCatalog.get().asInstanceOf[HiveCatalog]
          if (tableIdentifier.notExistsDB) {
            hiveCatalog.tableExists(this.toFlinkTableIdentifier(TableIdentifier(tableIdentifier.identifier, FireHiveConf.defaultDB)))
          } else {
            hiveCatalog.tableExists(this.toFlinkTableIdentifier(tableIdentifier))
          }
        } else false
      } (this.logger, catchLog = s"判断${tableIdentifier}是否为hive表失败", hook = false)
    }
  }

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
   * 将解析到的表信息添加到实时血缘中
   */
  @Internal
  protected def addCatalog(identifier: TableIdentifier, operation: Operation): Unit = {
    SQLLineageManager.setOperation(identifier, operation.toString)

    // Flink临时表血缘解析
    if (this.isTempView(identifier)) {
      SQLLineageManager.setCatalog(identifier, this.getCatalog(identifier).toString)
      SQLLineageManager.setTmpView(identifier, identifier.toString())
    }

    // Hive表血缘解析
    if (this.isHiveTable(identifier)) {
      val hiveTable = this.getHiveTable(identifier)
      if (hiveTable.isDefined) {
        val hive = hiveTable.get
        // 获取hive表额外信息
        val tableIdentifier = TableIdentifier(identifier.toString, hive.getDbName)
        SQLLineageManager.setPhysicalTable(tableIdentifier, tableIdentifier.toString)
        SQLLineageManager.setTmpView(tableIdentifier, tableIdentifier.toString)
        SQLLineageManager.setCatalog(tableIdentifier, this.getCatalog(identifier).toString)
        if (hive.getSd != null) {
          // 获取表存储路径
          SQLLineageManager.setCluster(tableIdentifier, hive.getSd.getLocation)
          // 获取字段列表
          if (hive.getSd.getCols.nonEmpty) {
            val fields = hive.getSd.getCols.map(schema => (schema.getName, schema.getType))
            SQLLineageManager.setColumns(tableIdentifier, fields)
          }
        }
        // 获取分区列表
        if (hive.getPartitionKeys.nonEmpty) {
          val partitions = hive.getPartitionKeys.map(schema => (schema.getName, schema.getType))
          SQLLineageManager.setPartitions(tableIdentifier, partitions)
        }
      }
    }
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

  /**
   * 解析SQL中的分区信息
   */
  @Internal
  protected def parsePartitions(sqlIdentifier: SqlIdentifier, partitionsNode: Seq[SqlNodeList]): Unit = {
    val tableIdentifier = this.toFireTableIdentifier(sqlIdentifier, true)
    val partitions = partitionsNode.flatMap(sqlNodeList => sqlNodeList.getList.map(sqlNode => sqlNode.asInstanceOf[SqlProperty])).map(partitionNode => partitionNode.getKeyString -> partitionNode.getValueString).toMap
    if (partitions.nonEmpty) {
      SQLLineageManager.setPartitions(tableIdentifier, partitions.toSeq)
    }
  }

  /**
   * 解析flink create table语句
   */
  @Internal
  protected def parseCreateTable(createTable: SqlCreateTable): Unit = {
    // create table语句
    val tableIdentifier = this.toFireTableIdentifier(createTable.getTableName, false)
    SQLLineageManager.setTmpView(tableIdentifier, tableIdentifier.identifier)
    this.parseSqlNode(createTable.getTableName, Operation.CREATE_TABLE)

    val tableLike = createTable.getTableLike
    if (!tableLike.isPresent) {
      // 解析建表语句中的with参数列表
      val properties = this.parseOptions(tableIdentifier, createTable.getPropertyList)
      // 解析建表语句中的字段列表
      this.parseColumns(tableIdentifier, createTable.getColumnList)

      val catalog = properties.getOrElse("connector", "")
      if (noEmpty(catalog)) {
        SQLLineageManager.setCatalog(tableIdentifier, catalog)
        catalog match {
          case "kafka" => this.parseKafkaConnector(tableIdentifier, properties)
          case "jdbc" => this.parseJDBCConnector(tableIdentifier, properties)
          case "fire-rocketmq" => this.parseRocketMQConnector(tableIdentifier, properties)
          case _ =>
        }
      }
    } else {
      // create table like语句
      this.parseSqlNode(tableLike.get(), Operation.SELECT)
    }
  }

  /**
   * 解析JDBC数据源
   */
  @Internal
  protected def parseJDBCConnector(tableIdentifier: TableIdentifier, properties: Map[JString, JString]): Unit = {
    val tableName = properties.getOrElse("table-name", "")
    SQLLineageManager.setPhysicalTable(tableIdentifier, tableName)
    val url = properties.getOrElse("url", "")
    SQLLineageManager.setCluster(tableIdentifier, FireJdbcConf.jdbcUrl(url))
    val username = properties.getOrElse("username", "")
    LineageManager.addDBSql(Datasource.JDBC.toString, url, username, "", Operation.CREATE_TABLE, Operation.SELECT)
  }

  /**
   * 解析RocketMQ数据源
   */
  @Internal
  protected def parseRocketMQConnector(tableIdentifier: TableIdentifier, properties: Map[JString, JString]): Unit = {
    val url = properties.getOrElse("rocket.brokers.name", "")
    SQLLineageManager.setCluster(tableIdentifier, FireRocketMQConf.rocketNameServer(url))
    val topic = properties.getOrElse("rocket.topics", "")
    SQLLineageManager.setPhysicalTable(tableIdentifier, topic)
    val groupId = properties.getOrElse("rocket.group.id", "")
    LineageManager.addMQDatasource(Datasource.ROCKETMQ.toString, url, topic, groupId, Operation.CREATE_TABLE, Operation.SOURCE)
  }

  /**
   * 解析kafka数据源
   */
  @Internal
  protected def parseKafkaConnector(tableIdentifier: TableIdentifier, properties: Map[JString, JString]): Unit = {
    val url = properties.getOrElse("properties.bootstrap.servers", "")
    SQLLineageManager.setCluster(tableIdentifier, FireKafkaConf.kafkaBrokers(url))
    val topic = properties.getOrElse("topic", "")
    SQLLineageManager.setPhysicalTable(tableIdentifier, topic)
    val groupId = properties.getOrElse("properties.group.id", "")
    LineageManager.addMQDatasource(Datasource.KAFKA.toString, url, topic, groupId, Operation.CREATE_TABLE, Operation.SOURCE)
  }

  /**
   * 解析hive建表语句
   */
  @Internal
  protected def parseHiveCreateTable(sqlCreateTable: SqlCreateTable): Unit = {
    // 解析表名
    val tableIdentifier = toFireHiveTableIdentifier(TableIdentifier(sqlCreateTable.getTableName.toString))
    this.addCatalog(tableIdentifier, Operation.CREATE_TABLE)
    // 解析表注释
    if (sqlCreateTable.getComment.isPresent) SQLLineageManager.setComment(tableIdentifier, sqlCreateTable.getComment.get().toString)
    // 解析使用到的字段列表
    this.parseColumns(tableIdentifier, sqlCreateTable.getColumnList)
    // 解析options信息
    this.parseOptions(tableIdentifier, sqlCreateTable.getPropertyList)
    this.parseSqlNode(sqlCreateTable.getTableName, Operation.CREATE_TABLE, true)
  }

  /**
   * 用于解析sql中的options
   * @param tableIdentifier
   * 表名
   * @param options
   * 选项信息
   */
  @Internal
  protected def parseOptions(tableIdentifier: TableIdentifier, options: SqlNodeList): Map[String, String] = {
    val props = options.getList.map(t => t.toString.replace("'", "").split("="))
      .filter(t => t.nonEmpty && t.length == 2).map(t => if (t.contains("password")) (t(0).trim, "******") else (t(0).trim, t(1).trim)).toMap
    SQLLineageManager.setOptions(tableIdentifier, props)
    props
  }

  /**
   * 解析字段列表信息
   * @param tableIdentifier
   * 表名
   * @param columnList
   * 字段列表
   */
  @Internal
  protected def parseColumns(tableIdentifier: TableIdentifier, columnList: SqlNodeList): Unit = {
    val columns = columnList.toList.map(sqlNode => {
      sqlNode.toString.replace("`", "").split(" ")
    }).filter(arr => arr.nonEmpty && arr.length == 2).map(t => (t(0), t(1)))
    SQLLineageManager.setColumns(tableIdentifier, columns)
  }
}
