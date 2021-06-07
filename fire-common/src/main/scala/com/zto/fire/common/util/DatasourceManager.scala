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

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}

import com.google.common.collect.EvictingQueue
import com.zto.fire.common.conf.FireFrameworkConf._
import com.zto.fire.common.enu.{Datasource, ThreadPoolType}
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
 * 用于统计当前任务使用到的数据源信息，包括MQ、DB等连接信息等
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-11-26 15:30
 */
private[fire] class DatasourceManager {
  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)
  // 用于存放当前任务用到的数据源信息
  private[this] lazy val datasourceMap = new ConcurrentHashMap[Datasource, util.HashSet[DatasourceDesc]]()
  // 用于收集来自不同数据源的sql语句，后续会异步进行SQL解析，考虑到分布式场景下会有很多重复的SQL执行，因此使用了线程不安全的队列即可满足需求
  private lazy val sqlQueue = EvictingQueue.create[DBSqlSource](buriedPointDatasourceMaxSize)
  private[this] lazy val threadPool = ThreadUtils.createThreadPool("DatasourceManager", ThreadPoolType.SCHEDULED)
  this.sqlParse()

  /**
   * 用于异步解析sql中使用到的表，并放到datasourceMap中
   */
  private[this] def sqlParse(): Unit = {
    if (buriedPointDatasourceEnable && threadPool != null) {
      threadPool.asInstanceOf[ScheduledExecutorService].scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = {
          val start = currentTime
          if (sqlQueue != null) {
            for (i <- 1 until sqlQueue.size()) {
              val sqlSource = sqlQueue.poll()
              if (sqlSource != null) {
                val tableNames = SQLUtils.tableParse(sqlSource.sql)
                if (tableNames != null && tableNames.nonEmpty) {
                  tableNames.filter(StringUtils.isNotBlank).foreach(tableName => {
                    add(Datasource.parse(sqlSource.datasource), DBDatasource(sqlSource.datasource, sqlSource.cluster, tableName, sqlSource.username, sqlSource.sink))
                  })
                }
              }
            }
            logger.debug(s"异步解析SQL埋点中的表信息,耗时：${timecost(start)}")
          }
        }
      }, buriedPointDatasourceInitialDelay, buriedPointDatasourcePeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 添加一个数据源描述信息
   */
  private[fire] def add(sourceType: Datasource, datasourceDesc: DatasourceDesc): Unit = {
    var set = this.datasourceMap.get(sourceType)
    if (set == null) {
      set = new util.HashSet[DatasourceDesc]()
    }
    set.add(datasourceDesc)
    this.datasourceMap.put(sourceType, set)
  }

  /**
   * 向队列中添加一条sql类型的数据源，用于后续异步解析
   */
  private[fire] def addSql(source: DBSqlSource): Unit = if (buriedPointDatasourceEnable) this.sqlQueue.offer(source)

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def get: util.Map[Datasource, util.HashSet[DatasourceDesc]] = this.datasourceMap
}

/**
 * 对外暴露API，用于收集并处理各种埋点信息
 */
private[fire] object DatasourceManager {
  private lazy val manager = new DatasourceManager

  /**
   * 添加一条sql记录到队列中
   *
   * @param datasource
   *             数据源类型
   * @param cluster
   *             集群信息
   * @param sink source or sink
   * @param username
   *             用户名
   * @param sql
   *             待解析的sql语句
   */
  private[fire] def addSql(datasource: String, cluster: String, username: String, sql: String, sink: Boolean = true): Unit = {
    this.manager.addSql(DBSqlSource(datasource, cluster, username, sql, sink))
  }

  /**
   * 添加一条DB的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群信息
   * @param sink
   * source or sink
   * @param tableName
   * 表名
   * @param username
   * 连接用户名
   */
  private[fire] def addDBDatasource(datasource: String, cluster: String, tableName: String, username: String = "", sink: Boolean = true): Unit = {
    this.manager.add(Datasource.parse(datasource), DBDatasource(datasource, cluster, tableName, username, sink))
  }

  /**
   * 添加一条MQ的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   * @param sink
   * product or consumer
   * @param topics
   * 主题列表
   * @param groupId
   * 消费组标识
   */
  private[fire] def addMQDatasource(datasource: String, cluster: String, topics: String, groupId: String, sink: Boolean = false): Unit = {
    this.manager.add(Datasource.parse(datasource), MQDatasource(datasource, cluster, topics, groupId, sink))
  }

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def get: util.Map[Datasource, util.HashSet[DatasourceDesc]] = this.manager.get
}

/**
 * 数据源描述
 */
trait DatasourceDesc

/**
 * 面向数据库类型的数据源，带有tableName
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param sink
 * true: sink false: source
 * @param tableName
 * 表名
 * @param username
 * 使用关系型数据库时作为jdbc的用户名，HBase留空
 */
case class DBDatasource(datasource: String, cluster: String, tableName: String, username: String = "", sink: Boolean = true) extends DatasourceDesc

/**
 * 面向数据库类型的数据源，需将SQL中的tableName主动解析
 *
 * @param datasource
 *            数据源类型，参考DataSource枚举
 * @param cluster
 *            数据源的集群标识
 * @param sink
 *            true: sink false: source
 * @param username
 *            使用关系型数据库时作为jdbc的用户名，HBase留空
 * @param sql 执行的SQL语句
 */
case class DBSqlSource(datasource: String, cluster: String, username: String, sql: String, sink: Boolean = true) extends DatasourceDesc

/**
 * MQ类型数据源，如：kafka、RocketMQ等
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param sink
 * true: sink false: source
 * @param topics
 * 使用到的topic列表
 * @param groupId
 * 任务的groupId
 */
case class MQDatasource(datasource: String, cluster: String, topics: String, groupId: String, sink: Boolean = false) extends DatasourceDesc