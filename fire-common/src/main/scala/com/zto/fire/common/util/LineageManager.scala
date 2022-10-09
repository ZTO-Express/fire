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

import com.zto.fire.common.bean.lineage.Lineage
import com.zto.fire.common.conf.FireFrameworkConf._
import com.zto.fire.common.enu.{Datasource, Operation, ThreadPoolType}
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

import java.util.Objects
import java.util.concurrent._
import scala.collection.{JavaConversions, mutable}

/**
 * 用于统计当前任务使用到的数据源信息，包括MQ、DB、hive等连接信息等
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-11-26 15:30
 */
private[fire] class LineageManager extends Logging {
  // 用于存放当前任务用到的数据源信息
  private[fire] lazy val lineageMap = new ConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]()
  private[fire] lazy val tableMetaSet = new CopyOnWriteArraySet[TableMeta]()
  // 用于收集来自不同数据源的sql语句，后续会异步进行SQL解析，考虑到分布式场景下会有很多重复的SQL执行，因此使用了线程不安全的队列即可满足需求
  private lazy val dbSqlQueue = new ConcurrentLinkedQueue[DBSqlSource]()
  // 用于解析数据源的异步定时调度线程
  private lazy val parserExecutor = ThreadUtils.createThreadPool("LineageManager", ThreadPoolType.SCHEDULED).asInstanceOf[ScheduledExecutorService]
  private var parseCount = 0
  // 用于收集各实时引擎执行的sql语句
  this.lineageParse()

  /**
   * 用于异步解析sql中使用到的表，并放到linageMap中
   */
  private[this] def lineageParse(): Unit = {
    if (lineageEnable) {
      this.parserExecutor.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = {
          parseCount += 1

          if (parseCount >= lineageRunCount && !parserExecutor.isShutdown) {
            logger.info(s"4. 异步解析实时血缘的定时任务采样共计：${lineageRunCount}次，即将退出异步线程")
            parserExecutor.shutdown()
          }

          // 1. 解析jdbc sql语句
          val start = currentTime
          tryWithLog {
            for (_ <- 1 until dbSqlQueue.size()) {
              val sqlSource = dbSqlQueue.poll()
              if (sqlSource != null) {
                val tableNames = SQLUtils.tableParse(sqlSource.sql)
                if (tableNames != null && tableNames.nonEmpty) {
                  tableNames.filter(StringUtils.isNotBlank).foreach(tableName => {
                    add(Datasource.parse(sqlSource.datasource), DBDatasource(sqlSource.datasource, sqlSource.cluster, tableName, sqlSource.username, operation = sqlSource.operation))
                  })
                }
              }
            }
          } (logger, s"1. 开始第${parseCount}/${lineageRunCount}次解析JDBC中的血缘信息", "jdbc血缘信息解析失败")

          // 2. 将解析好的引擎SQL血缘按Datasource进行分类
          tryWithLog {
            tableMetaSet.foreach(tableMeta => {
              val prop = tableMeta.properties
              val operationSet = Set(tableMeta.operation)

              tableMeta.datasource match {
                case Datasource.KAFKA => {
                  val dataSource = MQDatasource(Datasource.KAFKA.toString, prop.getOrDefault("properties.bootstrap.servers", ""), prop.getOrDefault("topic", ""), prop.getOrDefault("properties.group.id", ""), operationSet)
                  add(Datasource.KAFKA, dataSource)
                }
                case Datasource.FIRE_ROCKETMQ => {
                  val datasource = MQDatasource(Datasource.ROCKETMQ.toString, PropUtils.getString(prop.getOrDefault("rocket.brokers.name", "")), prop.getOrDefault("rocket.topics", ""), prop.getOrDefault("rocket.group.id", ""), operationSet)
                  add(Datasource.FIRE_ROCKETMQ, datasource)
                }
                case Datasource.JDBC => {
                  val driver = prop.getOrDefault("driver", "")
                  val url = prop.getOrDefault("url", "")
                  val user = prop.getOrDefault("username", "")
                  val datasource = DBDatasourceDetail(Datasource.JDBC.toString, url, tableMeta.tableName, user, operationSet)
                  add(Datasource.JDBC, datasource)
                }
                case _ => add(tableMeta.datasource, tableMeta)
              }
            })
          } (logger, s"2. 开始第${parseCount}/${lineageRunCount}次解析SQL中的血缘关系", "sql血缘关系解析失败")

          logger.info(s"3. 完成第${parseCount}/${lineageRunCount}次异步解析SQL埋点中的表信息，耗时：${elapsed(start)}")
        }
      }, lineageRunInitialDelay, lineageRunPeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 添加一个数据源描述信息
   */
  private[fire] def add(sourceType: Datasource, datasourceDesc: DatasourceDesc): Unit = {
    if (!lineageEnable) return
    val set = this.lineageMap.mergeGet(sourceType)(new JHashSet[DatasourceDesc]())
    if (set.isEmpty) set.add(datasourceDesc)
    val mergedSet = this.mergeDatasource(set, datasourceDesc)
    this.lineageMap.put(sourceType, mergedSet)
  }

  /**
   * merge相同数据源的对象
   */
  private[fire] def mergeDatasource(datasourceList: JHashSet[DatasourceDesc], datasourceDesc: DatasourceDesc): JHashSet[DatasourceDesc] = {
    val mergeSet = new CopyOnWriteArraySet[DatasourceDesc](datasourceList)
    mergeSet.foreach {
      case ds: DBDatasource => {
        if (datasourceDesc.isInstanceOf[DBDatasource]) {
          val target = datasourceDesc.asInstanceOf[DBDatasource]
          if (ds.equals(target)) {
            ds.operation.addAll(target.operation)
          } else {
            mergeSet.add(datasourceDesc)
          }
        }
      }
      case ds: DBDatasourceDetail => {
        if (datasourceDesc.isInstanceOf[DBDatasourceDetail]) {
          val target = datasourceDesc.asInstanceOf[DBDatasourceDetail]
          if (ds.equals(target)) {
            ds.operation.addAll(target.operation)
          } else {
            mergeSet.add(datasourceDesc)
          }
        }
      }
      case ds: DBSqlSource => {
        if (datasourceDesc.isInstanceOf[DBSqlSource]) {
          val target = datasourceDesc.asInstanceOf[DBSqlSource]
          if (ds.equals(target)) {
            ds.operation.addAll(target.operation)
          } else {
            mergeSet.add(datasourceDesc)
          }
        }
      }
      case ds: MQDatasource => {
        if (datasourceDesc.isInstanceOf[MQDatasource]) {
          val target = datasourceDesc.asInstanceOf[MQDatasource]
          if (ds.equals(target)) {
            ds.operation.addAll(target.operation)
          } else {
            mergeSet.add(datasourceDesc)
          }
        }
      }
      case _ =>
    }
    new JHashSet[DatasourceDesc](mergeSet)
  }

  /**
   * 向队列中添加一条sql类型的数据源，用于后续异步解析
   */
  private[fire] def addDBDataSource(source: DBSqlSource): Unit = if (lineageEnable && this.dbSqlQueue.size() <= lineMaxSize) this.dbSqlQueue.offer(source)

  /**
   * 收集执行的sql语句
   */
  private[fire] def addTableMeta(tableMetaSet: JSet[TableMeta]): Unit = if (lineageEnable) this.tableMetaSet.addAll(tableMetaSet)

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def get: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = this.lineageMap
}

/**
 * 对外暴露API，用于收集并处理各种埋点信息
 */
private[fire] object LineageManager extends Logging {
  private[fire] lazy val manager = new LineageManager

  /**
   * 添加一条sql记录到队列中
   *
   * @param datasource
   *             数据源类型
   * @param cluster
   *             集群信息
   * @param username
   *             用户名
   * @param sql
   *             待解析的sql语句
   */
  private[fire] def addDBSql(datasource: String, cluster: String, username: String, sql: String, operation: Operation*): Unit = {
    this.manager.addDBDataSource(DBSqlSource(datasource, cluster, username, sql, toOperationSet(operation: _*)))
  }

  /**
   * 添加解析后的TableMeta到队列中
   *
   * @param tableMeta 待解析的sql语句
   */
  def addTableMeta(tableMeta: JSet[TableMeta]): Unit = LineageManager.manager.addTableMeta(tableMeta)

  /**
   * 添加一条DB的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群信息
   * @param tableName
   * 表名
   * @param username
   * 连接用户名
   */
  private[fire] def addDBDatasource(datasource: String, cluster: String, tableName: String, username: String = "", operation: Operation): Unit = {
    this.manager.add(Datasource.parse(datasource), DBDatasource(datasource, cluster, tableName, username, toOperationSet(operation)))
  }

  /**
   * 添加多个数据源操作
   */
  private[fire] def toOperationSet(operation: Operation*): JHashSet[Operation] = {
    val operationSet = new JHashSet[Operation]
    operation.foreach(operationSet.add)
    operationSet
  }

  /**
   * 添加一条MQ的埋点信息
   *
   * @param datasource
   * 数据源类型
   * @param cluster
   * 集群标识
   * @param topics
   * 主题列表
   * @param groupId
   * 消费组标识
   */
  private[fire] def addMQDatasource(datasource: String, cluster: String, topics: String, groupId: String, operation: Operation*): Unit = {
    this.manager.add(Datasource.parse(datasource), MQDatasource(datasource, cluster, topics, groupId, toOperationSet(operation: _*)))
  }

  /**
   * 获取所有使用到的数据源
   */
  private[fire] def getDatasourceLineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = this.manager.get

  /**
   * 获取完整的实时血缘信息
   */
  private[fire] def getLineage: Lineage = {
    new Lineage(this.getDatasourceLineage, SQLLineageManager.getSQLLineage)
  }

  /**
   * 合并两个血缘map
   * @param current
   * 待合并的map
   * @param target
   * 目标map
   * @return
   * 合并后的血缘map
   */
  def mergeLineageMap(current: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]], target: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = {
    target.foreach(ds => {
      val datasourceDesc = current.mergeGet(ds._1)(ds._2)
      if (ds._2.nonEmpty) {
        ds._2.foreach(desc => {
          current.put(ds._1, this.manager.mergeDatasource(datasourceDesc, desc))
        })
      }
    })
    current
  }
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
 * @param tableName
 * 表名
 * @param username
 * 使用关系型数据库时作为jdbc的用户名，HBase留空
 * @param operation 数据源操作类型
 */
case class DBDatasource(datasource: String, cluster: String,
                        tableName: String, username: String = "",
                        operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[DBDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(tableName, target.tableName) && Objects.equals(username, target.username)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster, tableName, username)
}

/**
 * @param operation 针对表的具体操作类型
 */
case class DBDatasourceDetail(datasource: String, cluster: String,
                              tableName: String, username: String = "",
                              operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[DBDatasourceDetail]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(username, target.username)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster, tableName, username)
}

/**
 * 面向数据库类型的数据源，需将SQL中的tableName主动解析
 *
 * @param datasource
 *            数据源类型，参考DataSource枚举
 * @param cluster
 *            数据源的集群标识
 * @param username
 *            使用关系型数据库时作为jdbc的用户名，HBase留空
 * @param sql 执行的SQL语句
 * @param operation 数据源操作类型
 */
case class DBSqlSource(datasource: String, cluster: String, username: String,
                       sql: String, operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[DBSqlSource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(username, target.username) && Objects.equals(sql, target.sql)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster, username, sql)
}

/**
 * MQ类型数据源，如：kafka、RocketMQ等
 *
 * @param datasource
 * 数据源类型，参考DataSource枚举
 * @param cluster
 * 数据源的集群标识
 * @param operation
 * 数据源操作类型
 * @param topics
 * 使用到的topic列表
 * @param groupId
 * 任务的groupId
 */
case class MQDatasource(datasource: String, cluster: String, topics: String,
                        groupId: String, operation: JSet[Operation] = new JHashSet[Operation]) extends DatasourceDesc {

  override def equals(obj: Any): Boolean = {
    if (obj == null || getClass != obj.getClass) return false
    val target = obj.asInstanceOf[MQDatasource]
    Objects.equals(datasource, target.datasource) && Objects.equals(cluster, target.cluster) && Objects.equals(topics, target.topics) && Objects.equals(groupId, target.groupId)
  }

  override def hashCode(): Int = Objects.hash(datasource, cluster, topics, groupId)
}

/**
 * sql解析后的库表信息包装类
 *
 * @param dbName     数据库名称
 * @param tableName  表名
 * @param partition  分区信息
 * @param datasource 所属的catalog（default、hive等）
 * @param operation  针对该表的操作类型：SELECT、INSERT、DROP等
 * @param properties 标的属性，如with列表属性等
 */
case class TableMeta(dbName: String = "", tableName: String = "", partition: mutable.Map[String, String] = mutable.Map.empty, var datasource: Datasource = Datasource.VIEW, operation: Operation = Operation.SELECT, properties: mutable.Map[String, String] = mutable.Map.empty) extends DatasourceDesc