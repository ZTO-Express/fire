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

package com.zto.fire.spark.listener

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.exception.FireSparkException
import com.zto.fire.common.util.{ExceptionBus, Logging}
import com.zto.fire.spark.BaseSpark
import com.zto.fire.spark.acc.AccumulatorManager
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.sync.SyncSparkEngine
import org.apache.spark.SparkException
import org.apache.spark.scheduler._

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
  * Spark事件监听器桥
  * Created by ChengLong on 2018-05-19.
  */
private[fire] class FireSparkListener(baseSpark: BaseSpark) extends SparkListener with Logging {
  private[this] val module = "listener"
  private[this] val needRegister = new AtomicBoolean(false)
  // 用于统计stage失败的次数
  private[this] lazy val stageFailedCount = new AtomicLong(0)

  /**
   * 当SparkContext启动时触发
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    this.logger.info(s"Spark 初始化完成.")
    this.baseSpark.onApplicationStart(applicationStart)
  }

  /**
   * fire 框架退出
   */
  private[this] def exit: Unit = {
    try {
      this.baseSpark.after()
    } finally {
      this.baseSpark.shutdown(inListener = true)
    }
  }

  /**
   * 当Spark运行结束时执行
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    this.exit
    super.onApplicationEnd(applicationEnd)
  }

  /**
   * 当executor metrics更新时触发
   */
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = this.baseSpark.onExecutorMetricsUpdate(executorMetricsUpdate)

  /**
   * 当添加新的executor时，重新初始化内置的累加器
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    this.baseSpark.onExecutorAdded(executorAdded)
    if (this.baseSpark.jobType != JobType.SPARK_CORE) this.needRegister.compareAndSet(false, true)
    this.logger.debug(s"executor[${executorAdded.executorId}] added. host: [${executorAdded.executorInfo.executorHost}].", this.module)
  }

  /**
   * 当移除已有的executor时，executor数递减
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    this.baseSpark.onExecutorRemoved(executorRemoved)
    this.logger.debug(s"executor[${executorRemoved.executorId}] removed. reason: [${executorRemoved.reason}].", this.module)
  }

  /**
   * 当环境信息更新时触发
   */
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = this.baseSpark.onEnvironmentUpdate(environmentUpdate)

  /**
   * 当BlockManager添加时触发
   */
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = this.baseSpark.onBlockManagerAdded(blockManagerAdded)

  /**
   * 当BlockManager移除时触发
   */
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = this.baseSpark.onBlockManagerRemoved(blockManagerRemoved)

  /**
   * 当block更新时触发
   */
  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = this.baseSpark.onBlockUpdated(blockUpdated)

  /**
   * 当job开始执行时触发
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = this.baseSpark.onJobStart(jobStart)

  /**
   * 当job执行完成时触发
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    this.baseSpark.onJobEnd(jobEnd)
    if (jobEnd != null && jobEnd.jobResult == JobSucceeded) {
      AccumulatorManager.addMultiTimer(module, "onJobEnd", "onJobEnd", "", "INFO", "", 1)
    } else {
      AccumulatorManager.addMultiTimer(module, "onJobEnd", "onJobEnd", "", "ERROR", "", 1)
      this.logger.error(s"job failed.", this.module)
    }
  }

  /**
   * 当stage提交以后触发
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = this.baseSpark.onStageSubmitted(stageSubmitted)

  /**
   * 当stage执行完成以后触发
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.baseSpark.onStageCompleted(stageCompleted)
    if (stageCompleted != null && stageCompleted.stageInfo.failureReason.isEmpty) {
      AccumulatorManager.addMultiTimer(module, "onStageCompleted", "onStageCompleted", "", "INFO", "", 1)
    } else {
      AccumulatorManager.addMultiTimer(module, "onStageCompleted", "onStageCompleted", "", "ERROR", "", 1)
      this.logger.error(s"stage failed. reason: " + stageCompleted.stageInfo.failureReason, this.module)
      AccumulatorManager.addLog(stageCompleted.stageInfo.failureReason.getOrElse(""))

      // 异常信息统一投递到Fire异常总线
      ExceptionBus.post(new FireSparkException(stageCompleted.stageInfo.failureReason.get))

      // spark.fire.stage.maxFailures参数用于控制stage允许的最大失败次数，小于等于零表示不开启，默认-1
      // 当配置为2时表示最多允许失败2个stage，当第三个stage失败时SparkSession退出
      if (this.stageFailedCount.addAndGet(1) > FireSparkConf.stageMaxFailures && FireSparkConf.stageMaxFailures > 0) this.exit
    }
  }

  /**
   * 当task开始执行时触发
   */
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = this.baseSpark.onTaskStart(taskStart)

  /**
   * 当从task获取计算结果时触发
   */
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = this.baseSpark.onTaskGettingResult(taskGettingResult)

  /**
   * 当task执行完成以后触发
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    this.baseSpark.onTaskEnd(taskEnd)
    if (taskEnd != null && taskEnd.reason != null && "Success".equalsIgnoreCase(taskEnd.reason.toString)) {
      AccumulatorManager.addMultiTimer(module, "onTaskEnd", "onTaskEnd", "", "INFO", "", 1)
    } else {
      AccumulatorManager.addMultiTimer(module, "onTaskEnd", "onTaskEnd", "", "ERROR", "", 1)
      this.logger.error(s"task failed.", this.module)
    }
  }

  /**
   * 当取消缓存RDD时触发
   */
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = this.baseSpark.onUnpersistRDD(unpersistRDD)

  /**
   * 用于注册内置累加器，每隔1分钟执行一次，延迟1分钟执行，默认执行10次
   */
  @Scheduled(fixedInterval = 60 * 1000, initialDelay = 60 * 1000, concurrent = false, repeatCount = 10)
  private[fire] def registerAcc: Unit = {
    if (this.needRegister.compareAndSet(true, false)) {
      AccumulatorManager.registerAccumulators(this.baseSpark.sc)
      SyncSparkEngine.syncDynamicConf(this.baseSpark.sc, this.baseSpark._conf)
      this.logger.info(s"完成系统累加器注册.", this.module)
    }
  }
}
