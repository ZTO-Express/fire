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

package com.zto.fire.common.util

import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

import java.util.concurrent._


/**
 * 线程相关工具类
 *
 * @author ChengLong 2019-4-25 15:17:55
 */
object ThreadUtils extends Logging {
  // 用于维护使用ThreadUtils创建的线程池对象，并进行统一的关闭
  private lazy val poolMap = new JConcurrentHashMap[String, ExecutorService]()
  private lazy val singlePool = this.createThreadPool("FireSinglePool", ThreadPoolType.SINGLE)
  private lazy val cachedPool = this.createThreadPool("FireCachedPool", ThreadPoolType.CACHED)
  private lazy val scheduledPool = this.createThreadPool("FireScheduledPool", ThreadPoolType.SCHEDULED, FireFrameworkConf.threadPoolSchedulerSize).asInstanceOf[ScheduledExecutorService]

  /**
   * 利用SingleThreadExecutor执行给定的函数
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   */
  def runAsSingle(fun: => Unit): Unit = {
    this.singlePool.execute(new Runnable {
      override def run(): Unit = fun
    })
  }

  /**
   * 利用CachedThreadPool执行给定的函数
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   */
  def run(fun: => Unit): Unit = {
    this.cachedPool.execute(new Runnable {
      override def run(): Unit = fun
      logger.debug(s"Invoke runAsThread as ${Thread.currentThread().getName}.")
    })
  }

  /**
   * 利用CachedThreadPool循环执行给定的函数
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   * @param delay
   * 循环调用间隔时间（单位s）
   */
  def runLoop(fun: => Unit, delay: Long = 10): Unit = {
    this.cachedPool.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          fun
          logger.debug(s"Loop invoke runAsThreadLoop as ${Thread.currentThread().getName}. Delay is ${delay}s.")
          Thread.sleep(delay * 1000)
        }
      }
    })
  }

  /**
   * 利用ScheduledThreadPool定时调度执行给定的函数
   *
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param rate
   * true：表示周期性的执行，不受上一个定时任务的约束
   * false：表示当上一次周期性任务执行成功后，period后开始执行
   * @param timeUnit
   * 时间单位，默认分钟
   */
  def schedule(fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES): Unit = {
    if (rate) {
      // 表示周期性的执行，不受上一个定时任务的约束
      this.scheduledPool.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = wrapFun()
      }, initialDelay, period, timeUnit)
    } else {
      // 表示当上一次周期性任务执行成功后，period后开始执行
      this.scheduledPool.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = wrapFun()
      }, initialDelay, period, timeUnit)
    }

    // 处理传入的函数
    def wrapFun(): Unit = {
      fun
      this.logger.debug(s"Loop invoke runAsSchedule as ${Thread.currentThread().getName}. Delay is ${period}${timeUnit.name()}.")
    }
  }

  /**
   * 表示当上一次周期性任务执行成功后
   * period后开始执行给定的函数fun
   *
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param timeUnit
   * 时间单位，默认分钟
   */
  def scheduleAtFixedRate(fun: => Unit, initialDelay: Long, period: Long, timeUnit: TimeUnit = TimeUnit.MINUTES): Unit = {
    this.schedule(fun, initialDelay, period, true, timeUnit)
  }

  /**
   * 表示当上一次周期性任务执行成功后，period后开始执行fun函数
   * 注：受上一个定时任务的影响
   *
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param timeUnit
   * 时间单位，默认分钟
   */
  def scheduleWithFixedDelay(fun: => Unit, initialDelay: Long, period: Long, timeUnit: TimeUnit = TimeUnit.MINUTES): Unit = {
    this.schedule(fun, initialDelay, period, false, timeUnit)
  }

  /**
   * 创建一个新的指定大小的调度线程池
   * 如果名称已存在，则直接返回对应的线程池
   *
   * @param poolName
   * 线程池标识
   * @param poolType
   * 线程池类型
   * @param poolSize
   * 线程池大小
   */
  def createThreadPool(poolName: String, poolType: ThreadPoolType = ThreadPoolType.FIXED, poolSize: Int = 1): ExecutorService = {
    require(StringUtils.isNotBlank(poolName), "线程池名称不能为空")
    if (this.poolMap.containsKey(poolName)) {
      this.poolMap.get(poolName)
    } else {
      val threadPool = poolType match {
        case ThreadPoolType.FIXED => Executors.newFixedThreadPool(poolSize)
        case ThreadPoolType.SCHEDULED => Executors.newScheduledThreadPool(poolSize)
        case ThreadPoolType.SINGLE => Executors.newSingleThreadExecutor()
        case ThreadPoolType.CACHED => Executors.newCachedThreadPool()
        case ThreadPoolType.WORK_STEALING => Executors.newWorkStealingPool()
        case _ => Executors.newFixedThreadPool(poolSize)
      }
      this.poolMap.put(poolName, threadPool)
      threadPool
    }
  }

  /**
   * 用于释放指定的线程池
   *
   * @param poolName
   * 线程池标识
   */
  def shutdown(poolName: String): Unit = {
    if (StringUtils.isNotBlank(poolName) && this.poolMap.containsKey(poolName)) {
      val threadPool = this.poolMap.get(poolName)
      if (threadPool != null && !threadPool.isShutdown) {
        threadPool.shutdownNow()
        this.logger.debug(s"关闭线程池：${poolName}")
      }
    }
  }

  /**
   * 用于释放指定的线程池
   */
  def shutdown(pool: ExecutorService): Unit = {
    if (pool != null && !pool.isShutdown) {
      pool.shutdown()
      this.logger.debug(s"关闭线程池：${pool}")
    }
  }

  /**
   * 用于释放所有线程池
   */
  private[fire] def shutdown: Unit = {
    val poolNum = this.poolMap.size()
    if (this.poolMap.size() > 0) {
      this.poolMap.foreach(pool => {
        if (pool != null && pool._2 != null && !pool._2.isShutdown) {
          pool._2.shutdownNow()
          this.logger.info(s"${FirePS1Conf.GREEN}---> 完成线程池[ ${pool._1} ]的资源回收. <---${FirePS1Conf.DEFAULT}")
        }
      })
    }
    this.logger.info(s"${FirePS1Conf.PINK}---> 完成所有线程池回收，总计：${poolNum}个. <---${FirePS1Conf.DEFAULT}")
  }
}
