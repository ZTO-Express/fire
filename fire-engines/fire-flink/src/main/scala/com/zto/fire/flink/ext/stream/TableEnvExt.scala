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

package com.zto.fire.flink.ext.stream

import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.util.FlinkSingletonFactory
import com.zto.fire.noEmpty
import org.apache.flink.table.api.{SqlDialect, TableEnvironment}
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.functions.ScalarFunction

import java.util.Optional

/**
 * 用于对Flink StreamTableEnvironment的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class TableEnvExt(tableEnv: TableEnvironment) extends TableApi {
  // 获取hive catalog
  lazy val hiveCatalog = this.getHiveCatalog

  /**
   * 尝试获取注册的hive catalog对象
   */
  private def getHiveCatalog: Optional[Catalog] = {
    // 如果使用fire框架，则通过指定的hive catalog名称获取catalog实例
    val catalog = this.tableEnv.getCatalog(FireHiveConf.hiveCatalogName)
    if (catalog.isPresent) catalog else {
      // 如果fire未使用fire框架，尝试获取名称包含hive的catalog
      val hiveCatalogName = this.tableEnv.listCatalogs().filter(_.contains("hive"))
      if (noEmpty(hiveCatalogName)) this.tableEnv.getCatalog(hiveCatalogName(0)) else Optional.empty()
    }
  }
}

trait TableApi {
  private lazy val tableEnv = FlinkSingletonFactory.getTableEnv
  // 获取默认的catalog
  lazy val defaultCatalog = this.tableEnv.getCatalog(FireFlinkConf.defaultCatalogName)

  /**
   * 注册自定义udf函数
   *
   * @param name
   * 函数名
   * @param function
   * 函数的实例
   */
  def udf(name: String, function: ScalarFunction): Unit = {
    this.tableEnv.registerFunction(name, function)
  }

  /**
   * 用于判断当前是否hive catalog
   */
  def isHiveCatalog: Boolean = this.tableEnv.getCurrentCatalog.toUpperCase.contains("HIVE")

  /**
   * 用于判断当前是否为默认的catalog
   */
  def isDefaultCatalog: Boolean = !this.isHiveCatalog

  /**
   * 使用hive catalog
   */
  def useHiveCatalog(hiveCatalog: String = FireHiveConf.hiveCatalogName): Unit = {
    this.tableEnv.useCatalog(hiveCatalog)
    this.tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
  }

  /**
   * 使用默认的catalog
   */
  def useDefaultCatalog: Unit = {
    this.tableEnv.useCatalog(FireFlinkConf.defaultCatalogName)
    this.tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
  }
}