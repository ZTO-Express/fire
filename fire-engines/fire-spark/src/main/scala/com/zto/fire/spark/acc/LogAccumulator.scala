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

package com.zto.fire.spark.acc

import com.zto.fire.common.conf.FireFrameworkConf
import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.ConcurrentLinkedQueue

/**
  * fire框架日志累加器
  *
  * @author ChengLong 2019-7-23 14:22:16
  */
private[fire] class LogAccumulator extends StringAccumulator {
  // 判断是否打开日志累加器
  override protected lazy val isEnable = FireFrameworkConf.accEnable && FireFrameworkConf.accLogEnable

  /**
   * 用于复制累加器
   */
  override def copy(): AccumulatorV2[String, ConcurrentLinkedQueue[String]] = new LogAccumulator
}
