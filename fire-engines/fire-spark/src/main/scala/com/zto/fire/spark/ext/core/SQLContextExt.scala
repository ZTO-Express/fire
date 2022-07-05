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

package com.zto.fire.spark.ext.core

import com.zto.fire._
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.jdbc.util.DBUtils
import com.zto.fire.spark.conf.FireSparkConf
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.util.Properties

/**
 * SQLContext与HiveContext扩展
 *
 * @param sqlContext
 * sqlContext对象
 * @author ChengLong 2019-5-18 10:52:00
 */
class SQLContextExt(sqlContext: SQLContext) {

  /**
   * 链式设置
   *
   * @return
   * SQLContext对象
   */
  def set(key: String, value: String): SQLContext = {
    sqlContext.setConf(key, value)
    sqlContext
  }

  /**
   * 执行一段Hive QL语句，注册为临时表，持久化到hive中
   *
   * @param sqlStr
   * SQL语句
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
    val dataFrame = sqlContext.sql(sqlStr)
    val dataFrameWriter = dataFrame.write.mode(saveMode)
    if (StringUtils.isNotBlank(partitionName)) {
      dataFrameWriter.partitionBy(partitionName).saveAsTable(tmpTableName)
    } else {
      dataFrameWriter.saveAsTable(tmpTableName)
    }
    dataFrame
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
    val dataFrame = sqlContext.sql(sqlStr)
    dataFrame.createOrReplaceTempView(tmpTableName)
    sqlContext.cacheTable(tmpTableName)
    dataFrame
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
    val dataFrame = sqlContext.sql(sqlStr)
    dataFrame.createOrReplaceTempView(tmpTableName)
    dataFrame
  }

  /**
   * 批量清空多张缓存表
   *
   * @param tables
   * 多个表名
   */
  def uncacheTables(tables: String*): Unit = {
    if (noEmpty(tables)) {
      tables.filter(StringUtils.isNotBlank).foreach(tableName => {
        if (sqlContext.isCached(tableName)) {
          sqlContext.uncacheTable(tableName)
        }
      })
    }
  }

  /**
   * 批量缓存多张表
   *
   * @param tables
   * 多个表名
   */
  def cacheTables(tables: String*): Unit = {
    tables.foreach(tableName => {
      sqlContext.cacheTable(tableName)
    })
  }

  /**
   * 删除指定的hive表
   *
   * @param tableNames
   * 多个表名
   */
  def dropHiveTable(tableNames: String*): Unit = {
    if (noEmpty(tableNames)) {
      tableNames.filter(StringUtils.isNotBlank).foreach(tableName => {
        sqlContext.sql(s"DROP TABLE IF EXISTS $tableName")
      })
    }
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
    if (noEmpty(tableName, partitions)) {
      partitions.foreach(ds => {
        this.addPartition(tableName, ds, FireHiveConf.partitionName)
      })
    }
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
    if (noEmpty(tableName, partition, partitionName)) {
      sqlContext.sql(s"ALTER TABLE $tableName ADD IF NOT EXISTS partition($partitionName='$partition')")
    }
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
    if (noEmpty(tableName, partition, partitionName)) {
      sqlContext.sql(s"ALTER TABLE $tableName DROP IF EXISTS partition($partitionName='$partition')")
    }
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
    if (StringUtils.isNotBlank(tableName) && partitions != null) {
      partitions.foreach(ds => {
        this.dropPartition(tableName, ds, FireHiveConf.partitionName)
      })
    }
  }

  /**
   * 根据给定的表创建新表
   *
   * @param srcTableName
   * 源表名
   * @param destTableName
   * 目标表名
   */
  def createTableAsSelect(srcTableName: String, destTableName: String): Unit = {
    if (StringUtils.isNotBlank(srcTableName) && StringUtils.isNotBlank(destTableName)) {
      sqlContext.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS $destTableName AS
           |SELECT * FROM $srcTableName
          """.stripMargin)
    }
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
    if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(destTableName)) {
      sqlContext.sql(
        s"""
           |create table $tableName like $destTableName
          """.stripMargin)
    }
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
    if (StringUtils.isNotBlank(srcTableName) && StringUtils.isNotBlank(destTableName) && StringUtils.isNotBlank(cols)) {
      sqlContext.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS $destTableName AS
           |SELECT $cols FROM $srcTableName
          """.stripMargin)
    }
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
    sqlContext.sql(
      s"""
         |INSERT INTO TABLE $destTableName partition($partitionName='$ds')
         |  SELECT $cols
         |    FROM $srcTableName
        """.stripMargin)
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
    val overwriteVal = if (overwrite) "OVERWRITE" else "INTO"
    sqlContext.sql(
      s"""
         |INSERT $overwriteVal TABLE $destTableName partition($partitionName='$ds')
         |  $querySQL
        """.stripMargin)
  }

  /**
   * 将sql执行结果插入到目标表指定分区中
   *
   * @param destTableName
   * 目标表名
   * @param querySQL
   * 查询语句
   */
  def insertIntoDymPartitionAsSelect(destTableName: String, querySQL: String, partitionName: String = FireHiveConf.partitionName): Unit = {
    sqlContext.sql(
      s"""
         |INSERT INTO TABLE $destTableName partition($partitionName)
         |  $querySQL
        """.stripMargin)
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
    if (StringUtils.isBlank(oldTableName) || StringUtils.isBlank(newTableName)) {
      return
    }
    val sql = s"ALTER TABLE $oldTableName RENAME TO $newTableName"
    sqlContext.sql(sql)
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
    if (StringUtils.isBlank(tableName) || StringUtils.isBlank(newDB)) {
      return
    }
    val allName = if (StringUtils.isNotBlank(oldDB) && tableName.indexOf(".") == -1) {
      s"$oldDB.$tableName"
    } else {
      tableName
    }
    this.dropHiveTable(s"$newDB.$tableName")
    val sql = s"ALTER TABLE $allName RENAME TO $newDB.$tableName"
    println(sql)
    sqlContext.sql(sql)
  }

  // ----------------------------------- 关系型数据库API ----------------------------------- //

  /**
   * 单线程加载一张关系型数据库表
   * 注：仅限用于小的表，不支持条件查询
   *
   * @param tableName
   * 关系型数据库表名
   * @param jdbcProps
   * 调用者指定的数据库连接信息，如果为空，则默认读取配置文件
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * DataFrame
   */
  def jdbcTableLoadAll(tableName: String, jdbcProps: Properties = null, keyNum: Int = 1): DataFrame = {
    sqlContext.read.jdbc(FireJdbcConf.jdbcUrl(keyNum), tableName, DBUtils.getJdbcProps(jdbcProps, keyNum))
  }

  /**
   * 指定load的条件，从关系型数据库中并行的load数据，并转为DataFrame
   *
   * @param tableName 数据库表名
   * @param predicates
   *                  并行load数据时，每一个分区load数据的where条件
   *                  比如：gmt_create >= '2019-06-20' AND gmt_create <= '2019-06-21' 和 gmt_create >= '2019-06-22' AND gmt_create <= '2019-06-23'
   *                  那么将两个线程同步load，线程数与predicates中指定的参数个数保持一致
   * @param keyNum
   *                  配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   *                  比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 查询结果集
   */
  def jdbcTableLoad(tableName: String, predicates: Array[String], jdbcProps: Properties = null, keyNum: Int = 1): DataFrame = {
    sqlContext.read.jdbc(FireJdbcConf.jdbcUrl(keyNum), tableName, predicates, DBUtils.getJdbcProps(jdbcProps, keyNum))
  }

  /**
   * 根据指定分区字段的范围load关系型数据库中的数据
   *
   * @param tableName
   * 表名
   * @param columnName
   * 表的分区字段
   * @param lowerBound
   * 分区的下边界
   * @param upperBound
   * 分区的上边界
   * @param numPartitions
   * 加载数据的并行度，默认为10，设置过大可能会导致数据库挂掉
   * @param jdbcProps
   * jdbc连接信息，默认读取配置文件
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   */
  def jdbcTableLoadBound(tableName: String, columnName: String, lowerBound: Long, upperBound: Long, numPartitions: Int = 10, jdbcProps: Properties = null, keyNum: Int = 1): DataFrame = {
    sqlContext.read.jdbc(FireJdbcConf.jdbcUrl(keyNum), tableName, columnName, lowerBound, upperBound, numPartitions, DBUtils.getJdbcProps(jdbcProps, keyNum))
  }

}