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

package com.zto.fire.core.plugin

import com.taobao.arthas.agent.attach.ArthasAgent
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.{FireUtils, Logging, ReflectionUtils, ThreadUtils}
import com.zto.fire.predef.{JHashMap, _}

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Arthas插件管理
 *
 * @author ChengLong 2021-11-2 14:45:43
 * @since 2.2.0
 */
private[fire] object ArthasManager extends Logging {
  private lazy val isStarted = new AtomicBoolean(false)
  private lazy val isStopped = new AtomicBoolean(true)
  // arthas启动需要消耗较长时间，使用inProcessing可避免在启动过程中执行stop/restart等命令
  private lazy val inProcessing = new AtomicBoolean(false)

  /**
   * 启动Arthas服务
   *
   * @param resourceId     用于标识分布式任务的master与slave
   * @param startContainer 是否在分布式环境下启用Arthas
   */
  def startArthas(resourceId: String, startContainer: Boolean): Unit = {
    requireNonEmpty(resourceId)("resourceId不能为空，arthas所监控的程序必须有标识！")

    if (resourceId.contains("container") && !startContainer) return
    this.startArthas(resourceId)
  }

  /**
   * 关闭Arthas相关服务
   */
  def stopArthas: Unit = {
    if (this.isStopped.compareAndSet(false, true) && this.inProcessing.compareAndSet(false, true)) {
      this.logger.info("开始关闭Arthas相关服务")
      tryFinallyWithReturn {
        val bootstrap = ReflectionUtils.getFieldByName(classOf[ArthasAgent], "bootstrap").get(null)
        if (bootstrap != null) {
          val bootstrapClass = bootstrap.getClass
          ReflectionUtils.getMethodByName(bootstrapClass, "reset").invoke(bootstrap)
          ReflectionUtils.getMethodByName(bootstrapClass, "destroy").invoke(bootstrap)
        }
      } {
        this.isStarted.compareAndSet(true, false)
        this.inProcessing.compareAndSet(true, false)
      }(this.logger, "Arthas相关服务已关闭", "Arthas服务关闭失败！")
    }
  }

  /**
   * 启动Arthas服务
   *
   * @param resourceId 用于标识分布式任务的master与slave
   */
  def startArthas(resourceId: String): Unit = {
    if (this.isStarted.compareAndSet(false, true) && this.inProcessing.compareAndSet(false, true)) {
      this.logger.info("开始启动Arthas相关服务")
      ThreadUtils.run {
        tryWithLog {
          val configMap = new JHashMap[String, String]()
          configMap.put("arthas.appName", s"${FireUtils.engine}@${FireFrameworkConf.driverClassName}")
          configMap.put("arthas.telnetPort", "0")
          configMap.put("arthas.httpPort", "0")
          configMap.put("arthas.agentId", s"${FireUtils.engine}@${FireFrameworkConf.driverClassName}_$resourceId")
          configMap.put("arthas.tunnelServer", FireFrameworkConf.arthasTunnelServerUrl)
          configMap.put("arthas.username", "fire")
          configMap.put("arthas.password", FireFrameworkConf.driverClassName)
          configMap.putAll(FireFrameworkConf.arthasConfMap)
          ArthasAgent.attach(configMap)
          this.isStopped.compareAndSet(true, false)
          this.inProcessing.compareAndSet(true, false)
        }(this.logger, tryLog = "<-- Arthas服务已启动 -->")
      }
    }
  }

  /**
   * 重启Arthas相关服务
   *
   * @param resourceId 用于标识分布式任务的master与slave
   */
  def restartArthas(resourceId: String): Unit = {
    this.stopArthas
    this.startArthas(resourceId)
  }
}
