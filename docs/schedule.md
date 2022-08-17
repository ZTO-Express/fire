<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 定时任务

　　Fire框架内部进一步封装了quart进行定时任务的声明与调度，使用方法和spring的@Scheduled注解类似。参考：[示例程序](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/schedule/ScheduleTest.scala)。基于该功能，可以很容易实现诸如定时加载与更新维表等功能，十分方便。

```scala
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
    this.logger.info("executorId=" + SparkUtils.getExecutorId + "=方法 test2() 每5秒执行" +                      DateFormatUtils.formatCurrentDateTime())
  }

  // 每天凌晨4点01将锁标志设置为false，这样下一个批次就可以先更新维表再执行sql
  @Scheduled(cron = "0 1 4 * * ?")
  def updateTableJob: Unit = this.lock.compareAndSet(true, false)
```

**注：**目前定时任务不支持flink任务在每个TaskManager端执行。

