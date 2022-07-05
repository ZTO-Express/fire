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

package com.zto.fire.flink.sync

import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.core.sync.SyncEngineConf
import com.zto.fire.flink.util.FlinkUtils
import com.zto.fire.predef._

/**
 * 获取Spark引擎的所有配置信息
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-02 11:12
 */
private[fire] class SyncFlinkEngineConf extends SyncEngineConf  {
  private lazy val globalConfiguration = "org.apache.flink.configuration.GlobalConfiguration"
  private lazy val environmentInformation = "org.apache.flink.runtime.util.EnvironmentInformation"
  private lazy val getSettings = "getSettings"

  /**
   * 获取Flink引擎的所有配置信息
   */
  override def syncEngineConf: Map[String, String] = {
    if (FlinkUtils.isJobManager) {
      // 如果是JobManager端，则需将flink参数和用户参数进行合并，并从合并后的settings中获取
      val clazz = Class.forName(this.globalConfiguration)
      if (ReflectionUtils.containsMethod(clazz, this.getSettings)) {
        return clazz.getMethod(this.getSettings).invoke(null).asInstanceOf[JMap[String, String]].toMap
      }
    } else if (FlinkUtils.isTaskManager) {
      // 启用分布式同步
      DistributeSyncManager.sync
      // 如果是TaskManager端，则flink会通过EnvironmentInformation将参数进行传递
      val clazz = Class.forName(this.environmentInformation)
      if (ReflectionUtils.containsMethod(clazz, this.getSettings)) {
        return clazz.getMethod(this.getSettings).invoke(null).asInstanceOf[JMap[String, String]].toMap
      }
    }
    new JHashMap[String, String]().toMap
  }
}
