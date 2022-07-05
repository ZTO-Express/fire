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

package com.zto.fire.flink.plugin

import com.zto.fire.common.util.OSUtils
import com.zto.fire.predef.{noEmpty, _}
import com.zto.fire.core.plugin.{ArthasLauncher, ArthasManager}
import com.zto.fire.flink.util.FlinkUtils

/**
 * Flink Arthas分布式启动器
 *
 * @author ChengLong 2021-11-11 10:48:55
 * @since 2.2.0
 */
private[fire] class FlinkArthasLauncher extends ArthasLauncher {

  /**
   * 用于判断是否符合启动Arthas的条件
   *
   * @param ip
   * 用于指定在指定的ip上启动Arthas
   * @return
   * true: 可以启动  false：不应当启动
   */
  private[this] def canDo(isDistribute: Boolean, ip: String): Boolean = {
    if (FlinkUtils.isJobManager) return true
    if (isDistribute && FlinkUtils.isTaskManager && (isEmpty(ip) || (noEmpty(ip) && ip.contains(OSUtils.getIp)))) true else false
  }

  /**
   * 热启动Arthas
   *
   * @param isDistribute
   * 是否在每个container端启动arthas
   * @param ip
   * 仅在某些主机上启动
   */
  override def hotStart(isDistribute: Boolean, ip: String): Unit = if (this.canDo(isDistribute, ip)) ArthasManager.startArthas(FlinkUtils.getResourceId)

  /**
   * 分布式热关闭Arthas相关服务
   *
   * @param isDistribute
   * 是否在每个container端停止arthas
   * @param ip
   * 仅在某些主机上启动
   */
  override def hotStop(isDistribute: Boolean, ip: String): Unit = if (this.canDo(isDistribute, ip)) ArthasManager.stopArthas

  /**
   * 分布式热重启rthas相关服务
   *
   * @param isDistribute
   * 是否在每个container端停止arthas
   * @param ip
   * 仅在某些主机上启动
   */
  override def hotRestart(isDistribute: Boolean, ip: String): Unit = if (this.canDo(isDistribute, ip)) ArthasManager.restartArthas(FlinkUtils.getResourceId)
}
