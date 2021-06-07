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

package com.zto.fire.spark

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.PropUtils

/**
  * 实时平台Spark通用父类
  * Created by ChengLong on 2018-03-28.
  */
class BaseSparkCore extends BaseSpark {
  override val jobType = JobType.SPARK_CORE

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param conf
    * Spark配置信息
    */
  override def init(conf: Any = null, args: Array[String] = null): Unit = {
    super.init(conf, args)
    this.process
  }

  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf: Unit = {
    PropUtils.load(FireFrameworkConf.SPARK_CORE_CONF_FILE)
  }

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    // 子类复写该方法实现业务处理逻辑
  }
}
