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

import com.zto.fire.predef._
import com.google.common.collect.HashBasedTable
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.conf.FireFrameworkConf.{lineageRunCount, lineageRunInitialDelay, lineageRunPeriod}
import com.zto.fire.common.enu.{Datasource, ThreadPoolType}
import com.zto.fire.common.util._
import com.zto.fire.spark.sync.DistributeSyncManager
import com.zto.fire.spark.task.SparkSchedulerManager
import com.zto.fire.spark.util.SparkUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable

/**
 * fire内置Spark累加器工具类
 *
 * @author ChengLong 2019-7-25 19:11:16
 */
private[fire] object AccumulatorManager extends Logging  {
  private lazy val executorId = SparkUtils.getExecutorId
  // 累加器名称，含有fire的名字将会显示在webui中
  private[this] val counterLabel = "fire-counter"
  private[fire] val counter = new LongAccumulator

  // String累加器
  private[this] val stringAccumulatorLabel = "stringAccumulator"
  private[fire] val stringAccumulator = new StringAccumulator

  // 血缘累加器
  private[this] val lineageAccumulatorLabel = "lineageAccumulator"
  private[fire] val lineageAccumulator = new LineageAccumulator

  // 同步累加器
  private[this] val syncAccumulatorLabel = "syncAccumulator"
  private[fire] val syncAccumulator = new SyncAccumulator

  // 日志累加器
  private[this] val logAccumulatorLabel = "logAccumulator"
  private[fire] val logAccumulator = new LogAccumulator

  // 多值累加器
  private[this] val multiCounterLabel = "fire-multiCounter"
  private[fire] val multiCounter = new MultiCounterAccumulator

  // timer累加器
  private[this] val multiTimerLabel = "multiTimer"
  private[fire] val multiTimer = new MultiTimerAccumulator

  // env累加器
  private[this] val envAccumulatorLabel = "envAccumulator"
  private[fire] val envAccumulator = new EnvironmentAccumulator

  // 累加器注册列表
  private[this] val accMap = Map(this.lineageAccumulatorLabel -> this.lineageAccumulator, this.syncAccumulatorLabel -> this.syncAccumulator,
    this.stringAccumulatorLabel -> this.stringAccumulator, this.logAccumulatorLabel -> this.logAccumulator, this.counterLabel -> this.counter,
    this.multiCounterLabel -> this.multiCounter, this.multiTimerLabel -> this.multiTimer, this.envAccumulatorLabel -> this.envAccumulator)

  // 获取当前任务的全类名
  private[this] lazy val jobClassName = SparkEnv.get.conf.get(FireFrameworkConf.DRIVER_CLASS_NAME, "")
  // 用于注册定时任务的列表
  private[this] val taskRegisterSet = mutable.HashSet[Object]()
  // 用于广播spark配置信息
  private[fire] var broadcastConf: Broadcast[SparkConf] = _
  // 用于解析数据源的异步定时调度线程
  private lazy val lineageThread = ThreadUtils.createThreadPool("LineageAccumulator", ThreadPoolType.SCHEDULED).asInstanceOf[ScheduledExecutorService]
  // 用于记录血缘解析运行的次数
  private lazy val lineageRunCount = new AtomicInteger()

  /**
   * 注册定时任务实例
   */
  def registerTasks(tasks: Object*): Unit = {
    if (tasks != null) {
      tasks.foreach(taskInstances => taskRegisterSet.add(taskInstances))
    }
  }

  /**
   * 将数据累加到count累加器中
   *
   * @param value
   * 累加值
   */
  def addCounter(value: Long): Unit = {
    if (FireUtils.isSparkEngine) {
      if (SparkEnv.get != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val countAccumulator = SparkEnv.get.conf.get(this.counterLabel, "")
        if (StringUtils.isNotBlank(countAccumulator)) {
          val counter: LongAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(countAccumulator)))
          counter.add(value)
        }
      } else {
        this.counter.add(value)
      }
    }
  }

  /**
   * 获取counter累加器的值
   *
   * @return
   * 累加结果
   */
  def getCounter: Long = this.counter.value

  /**
   * 将timeCost累加到日志累加器中
   *
   * @param log
   * TimeCost实例对象
   */
  def addLog(log: String): Unit = {
    if (isEmpty(log)) return
    if (FireUtils.isSparkEngine) {
      val env = SparkEnv.get
      if (env != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val logAccumulator = SparkEnv.get.conf.get(this.logAccumulatorLabel, "")
        if (StringUtils.isNotBlank(logAccumulator)) {
          val logAcc: LogAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(logAccumulator)))
          logAcc.add(log)
        }
      } else {
        this.logAccumulator.add(log)
      }
    }
  }

  /**
   * 将系统信息累加到同步累加器中
   *
   * @param json
   * 通信消息
   */
  private[fire] def addSync(json: String): Unit = {
    if (isEmpty(json)) return
    if (FireUtils.isSparkEngine) {
      val env = SparkEnv.get
      if (env != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val syncAccumulator = SparkEnv.get.conf.get(this.syncAccumulatorLabel, "")
        if (StringUtils.isNotBlank(syncAccumulator)) {
          val syncAcc: SyncAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(syncAccumulator)))
          syncAcc.add(json)
        }
      } else {
        this.syncAccumulator.add(json)
      }
    }
  }

  /**
   * 将血缘信息添加到累加器中
   */
  private[fire] def addLineage(lineageMap: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]): Unit = {
    if (isEmpty(lineageMap)) return
    if (FireUtils.isSparkEngine) {
      val env = SparkEnv.get
      if (env != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val lineageAccumulator = SparkEnv.get.conf.get(this.lineageAccumulatorLabel, "")
        if (StringUtils.isNotBlank(lineageAccumulator)) {
          val lineageAcc: LineageAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(lineageAccumulator)))
          lineageAcc.add(lineageMap)
        }
      } else {
        this.lineageAccumulator.add(lineageMap)
      }
    }
  }

  /**
   * 将字符串等累加到String累加器中
   *
   * @param str
   * 字符串（json）
   */
  def addString(str: String): Unit = {
    if (isEmpty(str)) return
    if (FireUtils.isSparkEngine) {
      val env = SparkEnv.get
      if (env != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val stringAccumulator = SparkEnv.get.conf.get(this.stringAccumulatorLabel, "")
        if (StringUtils.isNotBlank(stringAccumulator)) {
          val logAcc: StringAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(stringAccumulator)))
          logAcc.add(str)
        }
      } else {
        this.stringAccumulator.add(str)
      }
    }
  }

  /**
   * 添加异常堆栈日志到累加器中
   *
   * @param exceptionList
   * 堆栈列表
   */
  def addExceptionLog(exceptionList: List[(String, Throwable)], count: Long): Unit = {
    exceptionList.foreach(t => this.addLog(exceptionStack(t)))

    /**
     * 转换throwable为堆栈信息
     */
    def exceptionStack(exceptionTuple: (String, Throwable)): String = {
      s"""
         |异常信息<< ip：${OSUtils.getIp} executorId：${executorId} 异常时间：${exceptionTuple._1} 累计：${count}次. >>
         |异常堆栈：${ExceptionBus.stackTrace(exceptionTuple._2)}
         |""".stripMargin
    }
  }

  /**
   * 获取日志累加器中的值
   *
   * @return
   * 日志累加值
   */
  def getLog: ConcurrentLinkedQueue[String] = this.logAccumulator.value

  /**
   * 获取字符串累加器中的值
   *
   * @return
   * 日志累加值
   */
  def getString: ConcurrentLinkedQueue[String] = this.stringAccumulator.value

  /**
   * 获取系统同步累加器中的值
   *
   * @return
   * 日志累加值
   */
  def getSync: ConcurrentLinkedQueue[String] = this.syncAccumulator.value

  /**
   * 获取Fire采集到的血缘信息
   */
  def getLineage: JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]] = this.lineageAccumulator.value

  /**
   * 将运行时信息累加到env累加器中
   *
   * @param envInfo
   * 运行时信息
   */
  def addEnv(envInfo: String): Unit = {
    if (FireUtils.isSparkEngine) {
      val env = SparkEnv.get
      if (env != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val envAccumulator = SparkEnv.get.conf.get(this.envAccumulatorLabel, "")
        if (StringUtils.isNotBlank(envAccumulator)) {
          val envAcc: EnvironmentAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(envAccumulator)))
          envAcc.add(envInfo)
        }
      } else {
        this.envAccumulator.add(envInfo)
      }
    }
  }

  /**
   * 获取env累加器中的运行时信息
   *
   * @return
   * 运行时信息
   */
  def getEnv: ConcurrentLinkedQueue[String] = this.envAccumulator.value

  /**
   * 将数据累加到multiCount累加器中
   *
   * @param value
   * 累加值
   */
  def addMultiCounter(key: String, value: Long): Unit = {
    if (FireUtils.isSparkEngine) {
      if (SparkEnv.get != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val countAccumulator = SparkEnv.get.conf.get(this.multiCounterLabel, "")
        if (StringUtils.isNotBlank(countAccumulator)) {
          val multiCounter: MultiCounterAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(countAccumulator)))
          multiCounter.add(key, value)
        }
      } else {
        this.multiCounter.add(key, value)
      }
    }
  }

  /**
   * 获取multiCounter累加器的值
   *
   * @return
   * 累加结果
   */
  def getMultiCounter: ConcurrentHashMap[String, Long] = this.multiCounter.value

  /**
   * 将数据累加到timer累加器中
   *
   * @param value
   * 累加值的key、value和时间的schema，默认为yyyy-MM-dd HH:mm:00
   */
  def addMultiTimer(key: String, value: Long, schema: String = DateFormatUtils.TRUNCATE_MIN): Unit = {
    if (FireUtils.isSparkEngine) {
      if (SparkEnv.get != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
        val timerAccumulator = SparkEnv.get.conf.get(this.multiTimerLabel, "")
        if (StringUtils.isNotBlank(timerAccumulator)) {
          val multiTimer: MultiTimerAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(timerAccumulator)))
          multiTimer.add(key, value, schema)
        }
      } else {
        this.multiTimer.add(key, value, schema)
      }
    }
  }

  /**
   * 用于构建复杂类型（json）的多时间维度累加器的key
   * 并将key作为多时间维度累加器的key
   *
   * @param value
   * 累加的值
   * @param cluster
   * 连接的集群名
   * @param module
   * 所在的模块
   * @param method
   * 所在的方法名
   * @param action
   * 执行的动作
   * @param sink
   * 作用的目标
   * @param level
   * 日志级别：INFO、ERROR
   * @return
   * 累加器的key（json格式）
   */
  def addMultiTimer(module: String, method: String, action: String, sink: String, level: String, cluster: String, value: Long): Unit = {
    if (FireUtils.isSparkEngine) {
      val multiKey = s"""{"cluster":"$cluster","module":"$module","method":"$method","action":"$action","sink":"$sink","level":"$level","jobClass":"$jobClassName"}"""
      this.addMultiTimer(multiKey, value)
    }
  }

  /**
   * 获取timer累加器的值
   *
   * @return
   * 累加结果
   */
  def getMultiTimer: HashBasedTable[String, String, Long] = this.multiTimer.value

  /**
   * 注册多个自定义累加器到每个executor
   *
   * @param sc
   * SparkContext
   * [key, accumulator]
   */
  private[fire] def registerAccumulators(sc: SparkContext): Unit = this.synchronized {
    if (sc != null && accMap != null && accMap.nonEmpty) {
      // 将定时任务所在类的实例广播到每个executor端
      val taskSet = sc.broadcast(taskRegisterSet)
      val broadcastConf = sc.broadcast(SparkEnv.get.conf)
      this.broadcastConf = broadcastConf
      // 序列化内置的累加器
      val accumulatorMap = accMap.map(accInfo => {
        // 注册每个累加器，必须是合法的名称并且未被注册过
        if (accInfo._2 != null && !accInfo._2.isRegistered) {
          if (StringUtils.isNotBlank(accInfo._1) && accInfo._1.contains("fire")) {
            sc.register(accInfo._2, accInfo._1)
          } else {
            sc.register(accInfo._2)
          }
        }
        (accInfo._1, SparkEnv.get.closureSerializer.newInstance().serialize(accInfo._2).array())
      })

      DistributeSyncManager.sync({
        this.broadcastConf = broadcastConf
        // 将序列化后的累加器放置到conf中
        accumulatorMap.foreach(accSer => SparkEnv.get.conf.set(accSer._1, StringsUtils.toHexString(accSer._2)))
        if (FireFrameworkConf.scheduleEnable) {
          // 从广播中获取到定时任务的实例，并在executor端完成注册
          val tasks = taskSet.value
          if (tasks != null && tasks.nonEmpty && !SparkSchedulerManager.getInstance().schedulerIsStarted()) {
            SparkSchedulerManager.getInstance().registerTasks(tasks.toArray: _*)
          }
        }
      }, false)
    }
  }

  /**
   * 分布式采集血缘依赖
   */
  private[fire] def collectLineage: Unit = {
    if (!FireFrameworkConf.accEnable || !FireFrameworkConf.lineageEnable) return

    this.lineageThread.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        if (SparkUtils.isDriver) {
          // driver端采集
          addLineage(LineageManager.getDatasourceLineage)
          // executor端分布式采集
          DistributeSyncManager.sync({
            addLineage(LineageManager.getDatasourceLineage)
          })

          if (lineageRunCount.incrementAndGet() > FireFrameworkConf.lineageRunCount) {
            logger.info(s"Spark分布式血缘解析与采集任务即将退出，总计运行：${lineageRunCount.get()}次")
            lineageThread.shutdown()
          }
          logger.info(s"完成Spark分布式血缘解析与采集：${lineageRunCount.get()}次")
        }
      }
    }, lineageRunInitialDelay + 10, lineageRunPeriod, TimeUnit.SECONDS)
  }
}
