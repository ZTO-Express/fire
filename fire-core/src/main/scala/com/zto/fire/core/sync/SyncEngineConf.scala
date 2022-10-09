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

package com.zto.fire.core.sync

import com.zto.fire.common.bean.lineage.Lineage
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.{DatasourceDesc, ReflectionUtils}
import com.zto.fire.predef._

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable

/**
 * 用于获取不同计算引擎的全局配置信息，同步到fire框架中，并传递到每一个分布式实例
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-02 10:48
 */
private[fire] trait SyncEngineConf extends SyncManager {
  protected val isCollect = new AtomicBoolean(false)
  this.collect

  /**
   * 获取引擎的所有配置信息（send）
   */
  def syncEngineConf: Map[String, String]

  /**
   * 在master端获取系统累加器中的数据
   */
  def syncLineage: Lineage

  /**
   * 同步引擎各个container的信息到累加器中
   */
  def collect: Unit
}

/**
 * 用于获取不同引擎的配置信息
 */
private[fire] object SyncEngineConfHelper extends SyncEngineConf {
  private lazy val syncEngineClass: Class[_] = try {
    Class.forName(FireFrameworkConf.confDeployEngine)
  } catch {
    case e: Exception =>
      logger.error(s"未找到引擎配置获取实现类${FireFrameworkConf.confDeployEngine}，无法进行配置同步", e)
      throw e
  }
  private lazy val instance = syncEngineClass.newInstance()

  /**
   * 通过反射获取不同引擎的配置信息
   */
  override def syncEngineConf: Map[String, String] = {
    if (syncEngineClass != null) {
      val method = syncEngineClass.getDeclaredMethod("syncEngineConf")
      ReflectionUtils.setAccessible(method)
      method.invoke(instance).asInstanceOf[immutable.Map[String, String]]
    } else Map.empty
  }

  /**
   * 同步引擎各个container的信息到master端（collect）
   */
  override def syncLineage: Lineage = {
    val method = syncEngineClass.getDeclaredMethod("syncLineage")
    ReflectionUtils.setAccessible(method)
    method.invoke(instance).asInstanceOf[Lineage]
  }

  /**
   * 同步引擎各个container的信息到master端（collect）
   */
  override def collect: Unit = {
    val method = syncEngineClass.getDeclaredMethod("collect")
    ReflectionUtils.setAccessible(method)
    method.invoke(instance)
  }
}
