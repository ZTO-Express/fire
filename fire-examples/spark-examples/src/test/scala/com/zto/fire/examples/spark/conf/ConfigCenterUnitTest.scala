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

package com.zto.fire.examples.spark.conf

import com.zto.fire.common.anno.{Config, TestStep}
import com.zto.fire.examples.spark.core.SparkTester
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.anno.Streaming
import com.zto.fire.spark.util.SparkUtils
import org.junit.Test

/**
 * 用于测试配置fire框架的优先级
 *
 * @author ChengLong 2022-05-16 16:14:21
 * @date 2022-05-16 16:14:26
 * @since 2.2.2
 */
@Config(
  """
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |fire.conf.test=java
    |fire.thread.pool.schedule.size=6
    |fire.conf.test=spark
    |""")
@Streaming(20) // spark streaming的批次时间
class ConfigCenterUnitTest extends SparkCore with SparkTester {

  /**
   * 配置信息打印
   *
   *  ================================
   *  fire.thread.pool.size=6
   *  fire.thread.pool.schedule.size=5
   *  fire.acc.timer.max.size=30
   *  fire.acc.log.max.size=22
   *  fire.jdbc.query.partitions=11
   *  fire.conf.test=spark
   *  ================================
   */
  @Test
  @TestStep(step = 1, desc = "测试配置优先级")
  def assertConf: Unit = {
    this.logger.warn(s"================runtime is ${SparkUtils.getExecutorId}================")

    val poolSize = this.conf.getInt("fire.thread.pool.size", -1)
    val scheduleSize = this.conf.getInt("fire.thread.pool.schedule.size", -1)
    val accSize = this.conf.getInt("fire.acc.timer.max.size", -1)
    val logSize = this.conf.getInt("fire.acc.log.max.size", -1)
    val partitions = this.conf.getInt("fire.jdbc.query.partitions", -1)
    val test = this.conf.getString("fire.conf.test")

    this.logger.warn(s"fire.thread.pool.size=$poolSize")
    this.logger.warn(s"fire.thread.pool.schedule.size=$scheduleSize")
    this.logger.warn(s"fire.acc.timer.max.size=$accSize")
    this.logger.warn(s"fire.acc.log.max.size=$logSize")
    this.logger.warn(s"fire.jdbc.query.partitions=$partitions")
    this.logger.warn(s"fire.conf.test=$test")

    assert(poolSize == 6)
    assert(scheduleSize == 5)
    assert(accSize == 30)
    assert(logSize == 22)
    assert(partitions == 11)
    assert(test.equals("spark"))

    this.logger.warn(s"=======================================")
  }
}
