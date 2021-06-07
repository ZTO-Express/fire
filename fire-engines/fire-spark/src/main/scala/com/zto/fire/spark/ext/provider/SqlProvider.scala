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

package com.zto.fire.spark.ext.provider

import com.zto.fire._
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.udf.UDFs
import com.zto.fire.spark.util.SparkSingletonFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream

/**
 * 为扩展层提供Spark SQL api
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:35
 */
trait SqlProvider extends SparkProvider {

  /**
   * 清理 RDD、DataFrame、Dataset、DStream、TableName 缓存
   * 等同于unpersist
   *
   * @param any
   * RDD、DataFrame、Dataset、DStream、TableName
   */
  def uncache(any: Any*): Unit = {
    if (any != null && any.nonEmpty) {
      any.foreach(elem => {
        if (elem != null) {
          if (elem.isInstanceOf[String]) {
            val tableName = elem.asInstanceOf[String]
            if (this.tableExists(tableName) && this.isCached(tableName)) {
              SparkSingletonFactory.getSparkSession.sqlContext.uncacheTables(tableName)
            }
          } else if (elem.isInstanceOf[Dataset[_]]) {
            elem.asInstanceOf[Dataset[_]].uncache
          } else if (elem.isInstanceOf[DataFrame]) {
            elem.asInstanceOf[DataFrame].uncache
          } else if (elem.isInstanceOf[RDD[_]]) {
            elem.asInstanceOf[RDD[_]].uncache
          } else if (elem.isInstanceOf[DStream[_]]) {
            elem.asInstanceOf[DStream[_]].uncache
          }
        }
      })
    }
  }

  /**
   * 清理 RDD、DataFrame、Dataset、DStream、TableName 缓存
   * 等同于uncache
   *
   * @param any
   * RDD、DataFrame、Dataset、DStream、TableName
   */
  def unpersist(any: Any*): Unit = {
    this.uncache(any: _*)
  }

  /**
   * 批量注册udf函数，包含系统内置的与用户自定义的
   */
  def registerUDF(): SparkSession = {
    UDFs.registerSysUDF(SparkSingletonFactory.getSparkSession)
    SparkSingletonFactory.getSparkSession
  }

  /**
   * 用于判断当前SparkSession下临时表或Hive表是否存在
   *
   * @param tableName
   * 表名
   * @return
   * true：存在 false：不存在
   */
  def tableExists(tableName: String): Boolean = {
    SparkSingletonFactory.getSparkSession.catalog.tableExists(tableName)
  }

  /**
   * 用于判断当前SparkSession下临时表或Hive表是否存在
   *
   * @param tableName
   * 表名
   * @return
   * true：存在 false：不存在
   */
  def tableExists(dbName: String, tableName: String): Boolean = {
    SparkSingletonFactory.getSparkSession.catalog.tableExists(dbName, tableName)
  }

  /**
   * 执行一段Hive QL语句，注册为临时表，持久化到hive中
   *
   * @param sqlStr
   * sql语句
   * @param tmpTableName
   * 临时表名
   * @param saveMode
   * 持久化的模式，默认为Overwrite
   * @param cache
   * 默认缓存表
   * @return
   * 生成的DataFrame
   */
  def sqlForPersistent(sqlStr: String, tmpTableName: String, partitionName: String, saveMode: SaveMode = SaveMode.valueOf(FireSparkConf.saveMode), cache: Boolean = true): DataFrame = {
    SparkSingletonFactory.getSparkSession.sqlContext.sqlForPersistent(sqlStr, tmpTableName, partitionName, saveMode, cache)
  }

  /**
   * 执行一段Hive QL语句，注册为临时表，并cache
   *
   * @param sqlStr
   * SQL语句
   * @param tmpTableName
   * 临时表名
   * @return
   * 生成的DataFrame
   */
  def sqlForCache(sqlStr: String, tmpTableName: String): DataFrame = {
    SparkSingletonFactory.getSparkSession.sqlContext.sqlForCache(sqlStr, tmpTableName)
  }

  /**
   * 执行一段Hive QL语句，注册为临时表
   *
   * @param sqlStr
   * SQL语句
   * @param tmpTableName
   * 临时表名
   * @return
   * 生成的DataFrame
   */
  def sqlNoCache(sqlStr: String, tmpTableName: String): DataFrame = {
    SparkSingletonFactory.getSparkSession.sqlContext.sqlNoCache(sqlStr, tmpTableName)
  }

  /**
   * 批量缓存多张表
   *
   * @param tables
   * 多个表名
   */
  def cacheTables(tables: String*): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.cacheTables(tables: _*)
  }

  /**
   * 判断表是否被缓存
   *
   * @param tableName
   * 表名
   * @return
   */
  def isCached(tableName: String): Boolean = {
    SparkSingletonFactory.getSparkSession.sqlContext.isCached(tableName)
  }

  /**
   * 判断表是否未被缓存
   *
   * @param tableName
   * 表名
   * @return
   */
  def isNotCached(tableName: String): Boolean = !this.isCached(tableName)

  /**
   * refresh给定的表
   *
   * @param tables
   * 表名
   */
  def refreshTables(tables: String*): Unit = {
    if (tables != null) {
      tables.filter(noEmpty(_)).foreach(table => SparkSingletonFactory.getSparkSession.catalog.refreshTable(table))
    }
  }

  /**
   * 缓存或刷新给定的表
   * 1. 当表未被cache时会首先进行cache
   * 2. 当表已被cache，再次调用会进行refresh操作
   *
   * @param tables
   * 待cache或refresh的表名集合
   */
  def cacheOrRefreshTables(tables: String*): Unit = {
    if (tables != null) {
      tables.filter(noEmpty(_)).foreach(table => {
        if (this.isNotCached(table)) this.cacheTables(table) else this.refreshTables(table)
      })
    }
  }

  /**
   * 删除指定的hive表
   *
   * @param tableNames
   * 多个表名
   */
  def dropHiveTable(tableNames: String*): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.dropHiveTable(tableNames: _*)
  }

  /**
   * 为指定表添加分区
   *
   * @param tableName
   * 表名
   * @param partitions
   * 分区
   */
  def addPartitions(tableName: String, partitions: String*): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.addPartitions(tableName, partitions: _*)
  }

  /**
   * 为指定表添加分区
   *
   * @param tableName
   * 表名
   * @param partition
   * 分区
   * @param partitionName
   * 分区字段名称，默认ds
   */
  def addPartition(tableName: String, partition: String, partitionName: String = FireHiveConf.partitionName): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.addPartition(tableName, partition, partitionName)
  }

  /**
   * 为指定表删除分区
   *
   * @param tableName
   * 表名
   * @param partition
   * 分区
   */
  def dropPartition(tableName: String, partition: String, partitionName: String = FireHiveConf.partitionName): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.dropPartition(tableName, partition, partitionName)
  }

  /**
   * 为指定表删除多个分区
   *
   * @param tableName
   * 表名
   * @param partitions
   * 分区
   */
  def dropPartitions(tableName: String, partitions: String*): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.dropPartitions(tableName, partitions: _*)
  }

  /**
   * 根据给定的表创建新表
   *
   * @param srcTableName
   * 源表
   * @param destTableName
   * 目标表
   */
  def createTableAsSelect(srcTableName: String, destTableName: String): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.createTableAsSelect(srcTableName, destTableName)
  }

  /**
   * 根据一张表创建另一张表
   *
   * @param tableName
   * 表名
   * @param destTableName
   * 目标表名
   */
  def createTableLike(tableName: String, destTableName: String): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.createTableLike(tableName, destTableName)
  }

  /**
   * 根据给定的表创建新表
   *
   * @param srcTableName
   * 来源表
   * @param destTableName
   * 目标表
   * @param cols
   * 多个列，逗号分隔
   */
  def createTableAsSelectFields(srcTableName: String, destTableName: String, cols: String): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.createTableAsSelectFields(srcTableName, destTableName, cols)
  }

  /**
   * 将数据插入到指定表的分区中
   *
   * @param srcTableName
   * 来源表
   * @param destTableName
   * 目标表
   * @param ds
   * 分区名
   * @param cols
   * 多个列，逗号分隔
   */
  def insertIntoPartition(srcTableName: String, destTableName: String, ds: String, cols: String, partitionName: String = FireHiveConf.partitionName): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.insertIntoPartition(srcTableName, destTableName, ds, cols, partitionName)
  }

  /**
   * 将sql执行结果插入到目标表指定分区中
   *
   * @param destTableName
   * 目标表名
   * @param ds
   * 分区名
   * @param querySQL
   * 查询语句
   */
  def insertIntoPartitionAsSelect(destTableName: String, ds: String, querySQL: String, partitionName: String = FireHiveConf.partitionName, overwrite: Boolean = false): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.insertIntoPartitionAsSelect(destTableName, ds, querySQL, partitionName, overwrite)
  }

  /**
   * 将sql执行结果插入到目标表指定分区中
   *
   * @param destTableName
   * 目标表名
   * @param querySQL
   * 查询sql语句
   */
  def insertIntoDymPartitionAsSelect(destTableName: String, querySQL: String, partitionName: String = FireHiveConf.partitionName): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.insertIntoDymPartitionAsSelect(destTableName, querySQL, partitionName)
  }

  /**
   * 修改表名
   *
   * @param oldTableName
   * 表名称
   * @param newTableName
   * 新的表名
   */
  def rename(oldTableName: String, newTableName: String): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.rename(oldTableName, newTableName)
  }

  /**
   * 将表从一个db移动到另一个db中
   *
   * @param tableName
   * 表名
   * @param oldDB
   * 老库名称
   * @param newDB
   * 新库名称
   */
  def moveDB(tableName: String, oldDB: String, newDB: String): Unit = {
    SparkSingletonFactory.getSparkSession.sqlContext.moveDB(tableName, oldDB, newDB)
  }
}
