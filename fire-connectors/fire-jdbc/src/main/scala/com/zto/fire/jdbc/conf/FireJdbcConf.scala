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

package com.zto.fire.jdbc.conf

import com.zto.fire.common.util.PropUtils

/**
 * 关系型数据库连接池相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:56
 */
private[fire] object FireJdbcConf {
  // c3p0连接池相关配置
  lazy val JDBC_URL = "db.jdbc.url"
  lazy val JDBC_URL_PREFIX = "db.jdbc.url.map."
  lazy val JDBC_DRIVER = "db.jdbc.driver"
  lazy val JDBC_USER = "db.jdbc.user"
  lazy val JDBC_PASSWORD = "db.jdbc.password"
  lazy val JDBC_ISOLATION_LEVEL = "db.jdbc.isolation.level"
  lazy val JDBC_MAX_POOL_SIZE = "db.jdbc.maxPoolSize"
  lazy val JDBC_MIN_POOL_SIZE = "db.jdbc.minPoolSize"
  lazy val JDBC_ACQUIRE_INCREMENT = "db.jdbc.acquireIncrement"
  lazy val JDBC_INITIAL_POOL_SIZE = "db.jdbc.initialPoolSize"
  lazy val JDBC_MAX_IDLE_TIME = "db.jdbc.maxIdleTime"
  lazy val JDBC_BATCH_SIZE = "db.jdbc.batch.size"
  lazy val JDBC_FLUSH_INTERVAL = "db.jdbc.flushInterval"
  lazy val JDBC_MAX_RETRY = "db.jdbc.max.retry"
  // fire框架针对jdbc操作后数据集的缓存策略
  lazy val FIRE_JDBC_STORAGE_LEVEL = "fire.jdbc.storage.level"
  // 通过JdbcConnector查询后将数据集放到多少个分区中，需根据实际的结果集做配置
  lazy val FIRE_JDBC_QUERY_REPARTITION = "fire.jdbc.query.partitions"

  // 默认的事务隔离级别
  lazy val jdbcIsolationLevel = "READ_UNCOMMITTED"
  // 数据库批量操作的记录数
  lazy val jdbcBatchSize = 1000
  // fire框架针对jdbc操作后数据集的缓存策略
  lazy val jdbcStorageLevel = PropUtils.getString(this.FIRE_JDBC_STORAGE_LEVEL, "memory_and_disk_ser").toUpperCase
  // 通过JdbcConnector查询后将数据集放到多少个分区中，需根据实际的结果集做配置
  lazy val jdbcQueryPartition = PropUtils.getInt(this.FIRE_JDBC_QUERY_REPARTITION, 10)

  // db.jdbc.url
  def url(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_URL, "", keyNum)
  // jdbc url与别名映射
  lazy val jdbcUrlMap = PropUtils.sliceKeys(this.JDBC_URL_PREFIX)
  // db.jdbc.driver
  def driverClass(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_DRIVER,"", keyNum)
  // db.jdbc.user
  def user(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_USER, "", keyNum = keyNum)
  // db.jdbc.password
  def password(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_PASSWORD, "", keyNum = keyNum)
  // 事务的隔离级别：NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, SERIALIZABLE，默认为READ_UNCOMMITTED
  def isolationLevel(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_ISOLATION_LEVEL, this.jdbcIsolationLevel, keyNum)
  // 批量操作的记录数
  def batchSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_BATCH_SIZE, this.jdbcBatchSize, keyNum)
  // 默认多少毫秒flush一次
  def jdbcFlushInterval(keyNum: Int = 1): Long = PropUtils.getLong(this.JDBC_FLUSH_INTERVAL, 1000, keyNum)
  // jdbc失败最大重试次数
  def maxRetry(keyNum: Int = 1): Long = PropUtils.getLong(this.JDBC_MAX_RETRY, 3, keyNum)
  // 连接池最小连接数
  def minPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_MIN_POOL_SIZE, 1, keyNum)
  // 连接池初始化连接数
  def initialPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_INITIAL_POOL_SIZE, 1, keyNum)
  // 连接池最大连接数
  def maxPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_MAX_POOL_SIZE, 5, keyNum)
  // 连接池每次自增连接数
  def acquireIncrement(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_ACQUIRE_INCREMENT, 1, keyNum)
  // 多久释放没有用到的连接
  def maxIdleTime(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_MAX_IDLE_TIME, 30, keyNum)

  /**
   * 根据给定的jdbc url别名获取对应的jdbc地址
   */
  def jdbcUrl(keyNum: Int = 1): String = {
    val url = this.url(keyNum)
    this.jdbcUrlMap.getOrElse(url, url)
  }
}
