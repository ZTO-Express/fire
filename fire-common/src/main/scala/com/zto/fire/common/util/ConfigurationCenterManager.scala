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

import com.zto.fire.common.conf.FireFrameworkConf
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
private[fire] object ConfigurationCenterManager extends Serializable {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

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
       |{"className": "$className", "url": "$rest", "fireVersion": "${FireFrameworkConf.fireVersion}", "zrcKey": "${FireFrameworkConf.configCenterSecret}"}
      """.stripMargin
  }

  /**
   * 调用外部配置中心接口获取配合信息
   */
  def invokeConfigCenter(className: String): Unit = {
    if (!FireFrameworkConf.configCenterEnable || (OSUtils.isLocal && !FireFrameworkConf.configCenterLocalEnable)) return

    val param = buildRequestParam(className)
    var conf = ""
    try {
      conf = HttpClientUtils.doPost(FireFrameworkConf.configCenterProdAddress, param)
    } catch {
      case e: Exception => {
        this.logger.error("调用配置中心接口失败，开始尝试调用测试环境配置中心接口。", e)
        try {
          conf = HttpClientUtils.doPost(FireFrameworkConf.configCenterTestAddress, param)
        } catch {
          case e: Exception => {
            this.logger.error("无法从配置中心获取到该任务的配置信息，如遇配置中心注册接口不可用，仍需紧急发布，请将配置中心中的配置复制到当前任务的配置文件中，并通过以下配置关闭获取配置中心配置的接口，并重启任务：spark.fire.config_center.enable=false", e)
            throw e
          }
        }
      }
    } finally {
      if (StringUtils.isNotBlank(conf)) {
        this.logger.info(s"成功获取配置中心配置信息：${conf}")
        val map = JSONUtils.parseObject(conf, classOf[JMap[String, Object]])
        if (map.containsKey("code") && map.get("code").asInstanceOf[Int] == 200) {
          if (map.containsKey("content")) {
            val contentMap = map.get("content").asInstanceOf[JMap[String, String]]
            if (contentMap != null && contentMap.nonEmpty) {
              PropUtils.setProperties(contentMap)
            }
          }
        }
      }
    }
  }
}
