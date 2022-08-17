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

import com.zto.fire._
import com.zto.fire.common.anno.{Config, Scheduled}
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.util.SparkUtils

/**
 * 用于测试定时任务
 *
 * @author ChengLong 2019年11月5日 17:27:20
 * @since 0.3.5
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Config(
  """
    |spark.fire.task.schedule.enable         =       true
    |# 定时任务黑名单，配置方法名，多个以逗号分隔，配置的方法将不再被定时任务定时拉起
    |spark.fire.scheduler.blacklist          =       jvmMonitor,setConf2,registerAcc
    |""")
object ScheduleTest extends SparkStreaming {

  /**
   * 声明了@Scheduled注解的方法是定时任务方法，会周期性执行
   *
   * @cron cron表达式
   * @scope 默认同时在driver端和executor端执行，如果指定了driver，则只在driver端定时执行
   * @concurrent 上一个周期定时任务未执行完成时是否允许下一个周期任务开始执行
   * @startAt 用于指定第一次开始执行的时间
   * @initialDelay 延迟多长时间开始执行第一次定时任务
   */
  @Scheduled(cron = "0/5 * * * * ?", scope = "driver", concurrent = false, startAt = "2021-01-21 11:30:00", initialDelay = 60000)
  def loadTable: Unit = {
    this.logger.info("更新维表动作")
  }

  /**
   * 只在driver端执行，不允许同一时刻同时执行该方法
   * startAt用于指定首次执行时间
   */
  @Scheduled(cron = "0/5 * * * * ?", scope = "all", concurrent = false)
  def test2: Unit = {
    this.logger.info("executorId=" + SparkUtils.getExecutorId + "====方法 test2() 每5秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }


  // 每天凌晨4点01将锁标志设置为false，这样下一个批次就可以先更新维表再执行sql
  @Scheduled(cron = "0 1 4 * * ?")
  def updateTableJob: Unit = this.lock.compareAndSet(true, false)

  // 用于缓存变更过的维表，只有当定时任务将标记设置为可更新时才会真正拉取最新的表
  def cacheTable: Unit = {
    // 加载完成维表以后上锁
    if (this.lock.compareAndSet(false, true)) {
      this.fire.uncache("test")
      this.fire.cacheTables("test")
    }
  }

  override def process: Unit = {
    // 用于注册其他类下带有@Scheduler标记的方法
    this.registerSchedule(new Tasks)
    // 重复注册的任务会自动去重
    this.registerSchedule(new Tasks)

    // 更新并缓存维表动作，具体要根据锁的标记判断是否执行
    this.cacheTable
  }
}
