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

package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils

/**
 * hive相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:02
 */
private[fire] object FireHiveConf {
  lazy val HIVE_CLUSTER = "hive.cluster"
  // hive版本号
  lazy val HIVE_VERSION = "hive.version"
  // hive的catalog名称
  lazy val HIVE_CATALOG_NAME = "hive.catalog.name"
  lazy val HIVE_CLUSTER_MAP_PREFIX = "fire.hive.cluster.map."
  lazy val HIVE_SITE_PATH_MAP_PREFIX = "fire.hive.site.path.map."
  lazy val HIVE_CONF_PREFIX = "hive.conf."
  // 默认的库名
  lazy val DEFAULT_DATABASE_NAME = "fire.hive.default.database.name"
  // 默认的数据库名称
  lazy val dbName = "tmp"
  // 默认的分区名称
  lazy val DEFAULT_TABLE_PARTITION_NAME = "fire.hive.table.default.partition.name"
  // 默认的partition名称
  lazy val defaultPartitionName = "ds"

  // hive集群标识（batch/streaming/test）
  lazy val hiveCluster = PropUtils.getString(this.HIVE_CLUSTER, "")
  // 初始化hive集群名称与metastore映射
  private lazy val hiveMetastoreMap = PropUtils.sliceKeys(this.HIVE_CLUSTER_MAP_PREFIX)
  // hive-site.xml存放路径映射
  private lazy val hiveSiteMap = PropUtils.sliceKeys(this.HIVE_SITE_PATH_MAP_PREFIX)
  // hive版本号
  lazy val hiveVersion = PropUtils.getString(this.HIVE_VERSION, "1.1.0")
  // hive catalog名称
  lazy val hiveCatalogName = PropUtils.getString(this.HIVE_CATALOG_NAME, "hive")
  // hive的set配置，如：this.spark.sql("set hive.exec.dynamic.partition=true")
  lazy val hiveConfMap = PropUtils.sliceKeys(this.HIVE_CONF_PREFIX)
  lazy val defaultDB = PropUtils.getString(this.DEFAULT_DATABASE_NAME, this.dbName)
  lazy val partitionName = PropUtils.getString(this.DEFAULT_TABLE_PARTITION_NAME, this.defaultPartitionName)

  /**
   * 根据hive集群名称获取metastore地址
   */
  def getMetastoreUrl: String = {
    this.hiveMetastoreMap.getOrElse(hiveCluster, hiveCluster)
  }

  /**
   * 获取hive-site.xml的存放路径
   *
   * @return
   * /path/to/hive-site.xml
   */
  def getHiveConfDir: String = {
    this.hiveSiteMap.getOrElse(hiveCluster, hiveCluster)
  }
}