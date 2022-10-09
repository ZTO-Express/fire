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

package com.zto.fire.flink.sync

import com.zto.fire._
import com.zto.fire.common.bean.lineage.Lineage
import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf}
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util._
import com.zto.fire.core.sync.LineageAccumulatorManager
import com.zto.fire.hbase.conf.FireHBaseConf
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.predef.{JConcurrentHashMap, JHashSet}

import java.lang.{Boolean => JBoolean}
import java.util.concurrent.atomic.AtomicLong

/**
 * 用于将各个TaskManager端数据收集到JobManager端
 *
 * @author ChengLong 2022-08-29 16:29:17
 * @since 2.3.2
 */
object FlinkLineageAccumulatorManager extends LineageAccumulatorManager {
  private lazy val lineageMap = new JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]()
  private lazy val counter = new AtomicLong()

  /**
   * 去重合并血缘信息
   */
  private def mergeLineage(lineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
    // 合并来自各个TaskManager端的血缘信息
    if (lineage.nonEmpty) merge(lineage)

    // 合并血缘管理器中的血缘信息
    if (LineageManager.getDatasourceLineage.nonEmpty) merge(LineageManager.getDatasourceLineage)

    /**
     * 合并血缘
     */
    def merge(lineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
      lineage.foreach(each => {
        var set = this.lineageMap.get(each._1)
        if (set == null) set = new JHashSet[DatasourceDesc]()

        // 兼容jackson反序列化不支持HashSet与case class的问题
        if (each._2.isInstanceOf[JArrayList[_]]) {
          val datasource = each._2.asInstanceOf[JArrayList[JMap[String, _]]]
          datasource.foreach(map => {
            if (map.containsKey("datasource")) {
              val datasource = map.getOrElse("datasource", "").toString.toUpperCase
              val cluster = map.getOrElse("cluster", "").toString
              val username = map.getOrElse("username", "").toString
              val tableName = map.getOrElse("tableName", "").toString
              val topics = map.getOrElse("topics", "").toString
              val groupId = map.getOrElse("groupId", "").toString
              val operationArr = map.getOrElse("operation", "[]").toString.replace("[", "").replace("]", "")
              val operation = operationArr.split(",").map(operation => com.zto.fire.common.enu.Operation.parse(operation)).toSet

              Datasource.parse(datasource) match {
                case Datasource.JDBC => set.add(DBDatasource(datasource, FireJdbcConf.jdbcUrl(cluster), tableName, username, operation = operation))
                case Datasource.HBASE => set.add(DBDatasource(datasource, FireHBaseConf.hbaseClusterUrl(cluster), tableName, username, operation = operation))
                case Datasource.KAFKA => set.add(MQDatasource(datasource, FireKafkaConf.kafkaBrokers(cluster), topics, groupId, operation = operation))
                case Datasource.ROCKETMQ => set.add(MQDatasource(datasource, FireRocketMQConf.rocketNameServer(cluster), topics, groupId, operation = operation))
                case _ =>
              }
            }
          })
        } else {
          each._2.filter(desc => desc.toString.contains("datasource")).foreach(desc => set.add(desc))
        }
        if (set.nonEmpty) this.lineageMap.put(each._1, set)
      })
    }
  }

  /**
   * 将血缘信息放到累加器中
   */
  override def add(lineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
    if (lineage.nonEmpty) this.mergeLineage(lineage)
  }

  /**
   * 累加Long类型数据
   */
  override def add(value: Long): Unit = this.counter.addAndGet(value)

  /**
   * 获取收集到的血缘消息
   */
  override def getValue: Lineage = {
    new Lineage(this.lineageMap , SQLLineageManager.getSQLLineage)
  }
}