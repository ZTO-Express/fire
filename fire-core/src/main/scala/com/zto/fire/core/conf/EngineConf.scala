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

package com.zto.fire.core.conf

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.ReflectionUtils
import org.slf4j.LoggerFactory

import scala.collection.immutable

/**
 * 用于获取不同计算引擎的全局配置信息，同步到fire框架中，并传递到每一个分布式实例
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-02 10:48
 */
private[fire] trait EngineConf {
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 获取引擎的所有配置信息
   */
  def getEngineConf: Map[String, String]
}

/**
 * 用于获取不同引擎的配置信息
 */
private[fire] object EngineConfHelper extends EngineConf {

  /**
   * 通过反射获取不同引擎的配置信息
   */
  override def getEngineConf: Map[String, String] = {
    var clazz: Class[_] = null
    try {
      clazz = Class.forName(FireFrameworkConf.confDeployEngine)
    } catch {
      case e: Exception => logger.error(s"未找到引擎配置获取实现类${FireFrameworkConf.confDeployEngine}，无法进行配置同步", e)
    }

    if (clazz != null) {
      val method = clazz.getDeclaredMethod("getEngineConf")
      ReflectionUtils.setAccessible(method)
      method.invoke(clazz.newInstance()).asInstanceOf[immutable.Map[String, String]]
    } else Map.empty
  }

}
