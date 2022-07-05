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

package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils

/**
 * 常量配置类
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:00
 */
private[fire] class FireConf {
  // 用于区分不同的流计算引擎类型
  private[fire] lazy val engine = PropUtils.engine

  // Fire框架相关配置
  val frameworkConf = FireFrameworkConf
  // kafka相关配置
  val kafkaConf = FireKafkaConf
  // rocketMQ相关配置
  val rocketMQConf = FireRocketMQConf
  // 颜色预定义
  val ps1Conf = FirePS1Conf
  // hive相关配置
  val hiveConf = FireHiveConf
}

object FireConf extends FireConf