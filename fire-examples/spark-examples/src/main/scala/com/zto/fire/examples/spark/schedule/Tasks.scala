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

package com.zto.fire.examples.spark.schedule

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.spark.util.SparkUtils

/**
 * 定时任务注册类
 * 1. 可序列化
 * 2. 方法不带任何参数
 *
 * @author ChengLong 2019年11月5日 17:29:35
 * @since 0.3.5
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
class Tasks extends Serializable {

  /**
   * 只在driver端执行，不允许同一时刻同时执行该方法
   * startAt用于指定首次执行时间
   */
  @Scheduled(cron = "0/15 * * * * ?", scope = "all", concurrent = false)
  def test5: Unit = {
    println("executorId=" + SparkUtils.getExecutorId + "====方法 test5() 每15秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }
}
