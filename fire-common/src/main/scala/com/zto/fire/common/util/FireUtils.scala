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

import com.zto.fire.predef._
import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}

/**
 * fire框架通用的工具方法
 * 注：该工具类中不可包含Spark或Flink的依赖
 *
 * @author ChengLong
 * @since 1.0.0
 * @create: 2020-05-17 10:17
 */
private[fire] object FireUtils extends Serializable with Logging {
  private[fire] var isSplash = false

  /**
   * 判断是否为spark引擎
   */
  def isSparkEngine: Boolean = "spark".equals(this.engine)

  /**
   * 判断是否为flink引擎
   */
  def isFlinkEngine: Boolean = "flink".equals(this.engine)

  /**
   * 获取当前实时任务所使用的计算引擎
   * @return
   * spark / flink
   */
  def engine: String = PropUtils.engine

  /**
   * 获取fire版本号
   */
  def fireVersion: String = FireFrameworkConf.fireVersion

  /**
   * 获取当前执行引擎的版本号
   * @return
   * spark-version / flink-version
   */
  def engineVersion: String = {
    tryWithReturn {
      val methodName = "getVersion"
      if (this.isSparkEngine) {
        val getVersionMethod = ReflectionUtils.getMethodByName("com.zto.fire.spark.util.SparkUtils", methodName)
        "spark version:" + getVersionMethod.invoke(null)
      } else {
        val getVersionMethod = ReflectionUtils.getMethodByName("com.zto.fire.flink.util.FlinkUtils", methodName)
        "flink version:" + getVersionMethod.invoke(null)
      }
    } (this.logger, catchLog = "未获取到引擎版本信息")
  }

  /**
   * 当前任务实例的主类名：packageName+className
   */
  def mainClass: String = FireFrameworkConf.driverClassName

  /**
   * 用于在fire框架启动时展示信息
   */
  private[fire] def splash: Unit = {
    if (!isSplash) {
      val info =
        """
          |       ___                       ___           ___
          |     /\  \          ___        /\  \         /\  \
          |    /::\  \        /\  \      /::\  \       /::\  \
          |   /:/\:\  \       \:\  \    /:/\:\  \     /:/\:\  \
          |  /::\~\:\  \      /::\__\  /::\~\:\  \   /::\~\:\  \
          | /:/\:\ \:\__\  __/:/\/__/ /:/\:\ \:\__\ /:/\:\ \:\__\
          | \/__\:\ \/__/ /\/:/  /    \/_|::\/:/  / \:\~\:\ \/__/
          |      \:\__\   \::/__/        |:|::/  /   \:\ \:\__\
          |       \/__/    \:\__\        |:|\/__/     \:\ \/__/
          |                 \/__/        |:|  |        \:\__\
          |                               \|__|         \/__/     version
          |
          |""".stripMargin.replace("version", s"fire version:${FirePS1Conf.PINK + this.fireVersion + FirePS1Conf.GREEN} $engineVersion")

      this.logger.warn(FirePS1Conf.GREEN + info + FirePS1Conf.DEFAULT)
      this.isSplash = true
    }
  }
}
