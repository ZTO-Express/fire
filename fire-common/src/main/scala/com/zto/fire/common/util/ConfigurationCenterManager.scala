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

import com.zto.fire.common.bean.config.ConfigurationParam
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.ConfigureLevel
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
 * 配置中心管理器，用于读取配置中心中的配置信息
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-12 13:35
 */
private[fire] object ConfigurationCenterManager extends Serializable with Logging {
  private lazy val configCenterProperties: JMap[ConfigureLevel, JMap[String, String]] = new JHashMap[ConfigureLevel, JMap[String, String]]

  /**
   * 构建配置中心请求参数
   *
   * @param className
   * 当前任务主类名
   */
  private[this] def buildRequestParam(className: String): String = {
    val rest = FireFrameworkConf.fireRestUrl
    if (StringUtils.isBlank(rest)) this.logger.warn("Fire Rest Server 地址为空，将无法完成注册")
    s"""
       |{"className": "${className.replace("$", "")}", "url": "$rest", "fireVersion": "${FireFrameworkConf.fireVersion}", "zrcKey": "${FireFrameworkConf.configCenterSecret}", "engine": "${PropUtils.engine}"}
      """.stripMargin
  }

  /**
   * 通过参数调用指定的接口
   */
  private[this] def invoke(url: String, param: String): String = {
    try {
      HttpClientUtils.doPost(url, param)
    } catch {
      case _: Throwable => this.logger.error("调用配置中心接口失败，开始尝试调用测试环境配置中心接口。")
        ""
    }
  }

  /**
   * 调用外部配置中心接口获取配合信息
   */
  def invokeConfigCenter(className: String): JMap[ConfigureLevel, JMap[String, String]] = {
    if (!FireFrameworkConf.configCenterEnable || (OSUtils.isLocal && !FireFrameworkConf.configCenterLocalEnable)) return this.configCenterProperties

    val param = buildRequestParam(className)
    // 尝试从生产环境配置中心获取参数列表
    var json = this.invoke(FireFrameworkConf.configCenterProdAddress, param)
    // 如果生产环境接口调用失败，可能存在网络隔离，则从测试环境配置中心获取参数列表
    if (isEmpty(json)) json = this.invoke(FireFrameworkConf.configCenterTestAddress, param)
    if (isEmpty(json)) {
      // 考虑到任务的重要配置可能存放在配置中心，在接口不通的情况下发布任务存在风险，因此会强制任务退出
      this.logger.error("配置中心注册接口不可用导致任务发布失败。如仍需紧急发布，请确保任务配置与配置中心保存一直，并在common.properties中添加以下参数：fire.config_center.enable=false")
      System.exit(-1)
    } else {
      this.logger.info(s"成功获取配置中心配置信息：$json")
      val param = JSONUtils.parseObject[ConfigurationParam](json)
      if (noEmpty(param, param.getCode, param.getContent) && param.getCode == 200) {
        this.configCenterProperties.putAll(param.getContent)
        this.logger.debug("配置中心参数已生效")
      }
    }
    this.configCenterProperties
  }
}
