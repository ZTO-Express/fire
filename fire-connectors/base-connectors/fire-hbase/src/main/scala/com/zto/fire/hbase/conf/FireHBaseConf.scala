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

package com.zto.fire.hbase.conf

import java.util

import com.zto.fire.common.util.PropUtils
import com.zto.fire.predef._


/**
 * hbase相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:08
 */
private[fire] object FireHBaseConf {
  lazy val HBASE_BATCH = "fire.hbase.batch.size"
  lazy val HBBASE_COLUMN_FAMILY_KEY = "hbase.column.family"
  lazy val HBASE_MAX_RETRY = "hbase.max.retry"
  lazy val HBASE_CLUSTER_URL = "hbase.cluster"
  lazy val HBASE_DURABILITY = "hbase.durability"
  // fire框架针对hbase操作后数据集的缓存策略，配置列表详见：StorageLevel.scala（配置不区分大小写）
  lazy val FIRE_HBASE_STORAGE_LEVEL = "fire.hbase.storage.level"
  // 通过HBase scan后repartition的分区数
  @deprecated("use fire.hbase.scan.partitions", "v1.0.0")
  lazy val FIRE_HBASE_SCAN_REPARTITIONS = "fire.hbase.scan.repartitions"
  lazy val FIRE_HBASE_SCAN_PARTITIONS = "fire.hbase.scan.partitions"
  // hbase集群映射配置前缀
  lazy val hbaseClusterMapPrefix = "fire.hbase.cluster.map."
  // 是否开启HBase表存在判断的缓存
  lazy val TABLE_EXISTS_CACHE_ENABLE = "fire.hbase.table.exists.cache.enable"
  // 是否开启HBase表存在列表缓存的定时更新任务
  lazy val TABLE_EXISTS_CACHE_RELOAD_ENABLE = "fire.hbase.table.exists.cache.reload.enable"
  // 定时刷新缓存HBase表任务的初始延迟
  lazy val TABLE_EXISTS_CACHE_INITIAL_DELAY = "fire.hbase.table.exists.cache.initialDelay"
  // 定时刷新缓存HBase表任务的执行频率
  lazy val TABLE_EXISTS_CACHE_PERIOD = "fire.hbase.table.exists.cache.period"

  // hbase集群映射地址
  lazy val hbaseClusterMap: util.Map[String, String] = PropUtils.sliceKeys(this.hbaseClusterMapPrefix)
  // hbase java api 配置前缀
  lazy val hbaseConfPrefix = "fire.hbase.conf."

  // 是否开启HBase表存在判断的缓存
  def tableExistsCache(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.TABLE_EXISTS_CACHE_ENABLE, true, keyNum)
  // 是否开启HBase表存在列表缓存的定时更新任务
  def tableExistsCacheReload(keyNum: Int = 1): Boolean = PropUtils.getBoolean(this.TABLE_EXISTS_CACHE_RELOAD_ENABLE, true, keyNum)
  // 定时刷新缓存HBase表任务的初始延迟
  def tableExistCacheInitialDelay(keyNum: Int = 1): Long = PropUtils.getLong(this.TABLE_EXISTS_CACHE_INITIAL_DELAY, 60, keyNum)
  // 定时刷新缓存HBase表任务的执行频率
  def tableExistCachePeriod(keyNum: Int = 1): Long = PropUtils.getLong(this.TABLE_EXISTS_CACHE_PERIOD, 600, keyNum)
  // HBase操作默认的批次大小
  def hbaseBatchSize(keyNum: Int = 1): Int = PropUtils.getInt(this.HBASE_BATCH, 10000, keyNum)
  // hbase默认的列族名称，如果使用FieldName指定，则会被覆盖
  def familyName(keyNum: Int = 1): String = PropUtils.getString(this.HBBASE_COLUMN_FAMILY_KEY, "info", keyNum)
  // hbase操作失败最大重试次数
  def hbaseMaxRetry(keyNum: Int = 1): Long = PropUtils.getLong(this.HBASE_MAX_RETRY, 3, keyNum)
  // hbase集群名称
  def hbaseCluster(keyNum: Int = 1): String = PropUtils.getString(this.HBASE_CLUSTER_URL, "", keyNum)

  /**
   * 根据给定的HBase集群别名获取对应的hbase.zookeeper.quorum地址
   */
  def hbaseClusterUrl(keyNum: Int = 1): String = {
    val clusterName = this.hbaseCluster(keyNum)
    this.hbaseClusterMap.getOrElse(clusterName, clusterName)
  }

  def hbaseDurability(keyNum: Int = 1): String = PropUtils.getString(this.HBASE_DURABILITY, "", keyNum)

  // HBase结果集的缓存策略配置
  def hbaseStorageLevel(keyNum: Int = 1): String = PropUtils.getString(this.FIRE_HBASE_STORAGE_LEVEL, "memory_and_disk_ser", keyNum).toUpperCase

  // 通过HBase scan后repartition的分区数，默认1200
  def hbaseHadoopScanPartitions(keyNum: Int = 1): Int = {
    val partitions = PropUtils.getInt(this.FIRE_HBASE_SCAN_PARTITIONS, -1, keyNum)
    if (partitions != -1) partitions else PropUtils.getInt(this.FIRE_HBASE_SCAN_REPARTITIONS, 1200, keyNum)
  }
}