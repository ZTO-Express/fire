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
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.spark.util.SparkUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.util.Properties
import scala.reflect.ClassTag

/**
 * 为扩展层提供jdbc相关的api
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:48
 */
trait JdbcSparkProvider extends SparkProvider {

  /**
   * 执行查询操作，以RDD方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return 查询结果集
   */
  def jdbcQueryRDD[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, keyNum: Int = 1): RDD[Row] = {
    this.jdbcQueryDF(sql, params, keyNum).rdd
  }

  /**
   * 执行查询操作，以DataFrame方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * JavaBean类型
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return 查询结果集
   */
  def jdbcQueryDF[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, keyNum: Int = 1): DataFrame = {
    JdbcConnector.executeQuery(sql, params, keyNum = keyNum, callback = rs => {
      SparkUtils.resultSet2DataFrame(rs, keyNum)
    }).persist(StorageLevel.fromString(FireJdbcConf.jdbcStorageLevel))
  }


  /**
   * 将DataFrame数据保存到关系型数据库中
   *
   * @param dataFrame
   * DataFrame数据集
   * @param tableName
   * 关系型数据库表名
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def jdbcTableSave(dataFrame: DataFrame, tableName: String, saveMode: SaveMode = SaveMode.Append, jdbcProps: Properties = null, keyNum: Int = 1): Unit = {
    dataFrame.jdbcTableSave(tableName, saveMode, jdbcProps, keyNum)
  }

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
    this.spark.sqlContext.jdbcTableLoadAll(tableName, jdbcProps, keyNum)
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
    this.spark.sqlContext.jdbcTableLoad(tableName, predicates, jdbcProps, keyNum)
  }

  /**
   * 根据指定字段的范围load关系型数据库中的数据
   *
   * @param tableName
   * 表名
   * @param columnName
   * 表的分区字段
   * @param lowerBound
   * 分区的下边界
   * @param upperBound
   * 分区的上边界
   * @param jdbcProps
   * jdbc连接信息，默认读取配置文件
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   */
  def jdbcTableLoadBound(tableName: String, columnName: String, lowerBound: Long, upperBound: Long, numPartitions: Int = 10, jdbcProps: Properties = null, keyNum: Int = 1): DataFrame = {
    this.spark.sqlContext.jdbcTableLoadBound(tableName, columnName, lowerBound, upperBound, keyNum, jdbcProps, keyNum)
  }

  /**
   * 将DataFrame中指定的列写入到jdbc中
   * 调用者需自己保证DataFrame中的列类型与关系型数据库对应字段类型一致
   *
   * @param dataFrame
   * 将要插入到关系型数据库中原始的数据集
   * @param sql
   * 关系型数据库待执行的增删改sql
   * @param fields
   * 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序
   * 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
   * @param batch
   * 每个批次执行多少条
   * @param keyNum
   * 对应配置文件中指定的数据源编号
   */
  def jdbcBatchUpdateDF(dataFrame: DataFrame, sql: String, fields: Seq[String] = null, batch: Int = FireJdbcConf.batchSize(), keyNum: Int = 1): Unit = {
    require(dataFrame != null && StringUtils.isNotBlank(sql), "执行jdbcBatchUpdateDF失败，dataFrame或sql为空")
    dataFrame.jdbcBatchUpdate(sql, fields, batch, keyNum)
  }
}
