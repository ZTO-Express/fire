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

import com.zto.fire.predef._
import com.zto.fire.common.conf.FireFrameworkConf

/**
 * Arthas启动器，可根据不同的引擎初始化不同的Arthas启动器实例
 *
 * @author ChengLong 2021-11-11 11:09:02
 * @since 2.2.0
 */
private[fire] object ArthasDynamicLauncher extends ArthasLauncher {
  private lazy val launcher: ArthasLauncher = this.install

  /**
   * 根据不同的引擎初始化对应的Arthas启动器
   *
   * @return
   */
  private[this] def install: ArthasLauncher = {
    val launcher = FireFrameworkConf.arthasLauncher
    requireNonEmpty(launcher)(s"Arthas启动器不能为空，请通过${FireFrameworkConf.FIRE_ARTHAS_LAUNCHER}进行配置")
    this.logger.info(s"Arthas启动器${launcher}初始化成功！")
    Class.forName(launcher).newInstance().asInstanceOf[ArthasLauncher]
  }


  /**
   * 热启动Arthas
   *
   * @param isDistribute
   * 是否在每个container端启动arthas
   * @param ip
   * 仅在某些主机上启动
   */
  override def hotStart(isDistribute: Boolean, ip: String): Unit = this.launcher.hotStart(isDistribute, ip)

  /**
   * 分布式热关闭Arthas相关服务
   *
   * @param isDistribute
   * 是否在每个container端停止arthas
   * @param ip
   * 仅在某些主机上启动
   */
  override def hotStop(isDistribute: Boolean, ip: String): Unit = this.launcher.hotStop(isDistribute, ip)

  /**
   * 分布式热重启rthas相关服务
   *
   * @param isDistribute
   * 是否在每个container端停止arthas
   * @param ip
   * 仅在某些主机上启动
   */
  override def hotRestart(isDistribute: Boolean, ip: String): Unit = this.launcher.hotRestart(isDistribute, ip)
}
