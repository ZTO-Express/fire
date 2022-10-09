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

package com.zto.fire.core

import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.{FireUtils, _}
import com.zto.fire.core.anno.lifecycle.{After, Before}
import com.zto.fire.core.conf.AnnoManager
import com.zto.fire.core.plugin.ArthasManager
import com.zto.fire.core.rest.{RestServerManager, SystemRestful}
import com.zto.fire.core.task.SchedulerManager
import com.zto.fire.predef._
import org.apache.log4j.{Level, Logger}
import spark.Spark

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

/**
 * 通用的父接口，提供通用的生命周期方法约束
 *
 * @author ChengLong 2020年1月7日 09:20:02
 * @since 0.4.1
 */
trait BaseFire extends Logging {
  // 任务启动时间戳
  protected[fire] val launchTime = FireUtils.launchTime
  // web ui地址
  protected[fire] var webUI, applicationId: String = _
  // main方法参数
  protected[fire] var args: Array[String] = _
  // 当前任务的类型标识
  protected[fire] val jobType = JobType.UNDEFINED
  // fire框架内置的restful接口
  private[fire] var systemRestful: SystemRestful = _
  // restful接口注册
  private[fire] var restfulRegister: RestServerManager = _
  // 用于子类的锁状态判断，默认关闭状态
  protected[fire] lazy val lock = new AtomicBoolean(false)
  // 是否已停止
  protected[fire] lazy val isStoped = new AtomicBoolean(false)
  // 当前任务的类名（包名+类名）
  protected[fire] lazy val className: JString = this.getClass.getName.replace("$", "")
  // 当前任务的类名
  protected[fire] lazy val driverClass: JString = this.getClass.getSimpleName.replace("$", "")
  // 默认的任务名称为类名
  protected[fire] var appName: JString = this.driverClass
  // 配置信息
  protected lazy val conf, $ = PropUtils
  this.boot()

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  private[fire] def boot(): Unit = {
    FireUtils.splash
    if (FireFrameworkConf.arthasEnable) ArthasManager.startArthas(this.resourceId, FireFrameworkConf.arthasContainerEnable)
    PropUtils.sliceKeys(FireFrameworkConf.FIRE_LOG_LEVEL_CONF_PREFIX).foreach(kv => Logger.getLogger(kv._1).setLevel(Level.toLevel(kv._2)))
    ExceptionBus.sendToMQ
  }

  /**
   * SQL语法校验，如果语法错误，则返回错误堆栈
   * @param sql
   * sql statement
   */
  def sqlValidate(sql: String): Try[Unit]

  /**
   * SQL语法校验
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  def sqlLegal(sql: String): Boolean

  /**
   * 获取任务的resourceId
   * @return
   * spark任务：driver/id  flink任务：JobManager/container_xxx
   */
  protected def resourceId: String

  /**
   * 在加载任务配置文件前将被加载
   */
  private[fire] def loadConf(): Unit = {
    // 加载配置文件
  }

  /**
   * 用于将不同引擎的配置信息、累计器信息等传递到executor端或taskmanager端
   */
  protected def deployConf(): Unit = {
    // 用于在分布式环境下分发配置信息
  }

  /**
   * 生命周期方法：用于在SparkSession初始化之前完成用户需要的动作
   * 注：该方法会在进行init之前自动被系统调用
   *
   * @param args
   * main方法参数
   */
  def before(args: Array[String]): Unit = {
    // 生命周期方法，在init之前被调用
    AnnoManager.lifeCycleAnno(this, classOf[Before])
  }

  /**
   * 生命周期方法：初始化运行信息
   *
   * @param conf 配置信息
   * @param args main方法参数
   */
  def init(conf: Any = null, args: Array[String] = null): Unit = {
    this.before(args)
    FireUtils._jobType = this.jobType
    this.logger.info(s" ${FirePS1Conf.YELLOW}---> 完成用户资源初始化，任务类型：${this.jobType.getJobTypeDesc} <--- ${FirePS1Conf.DEFAULT}")
    this.args = args
    this.createContext(conf)
  }

  /**
   * 创建计算引擎运行时环境
   *
   * @param conf
   * 配置信息
   */
  private[fire] def createContext(conf: Any): Unit

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  def process(): Unit

  /**
   * 生命周期方法：依次调用process方法以及加了注解的业务逻辑处理方法
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  protected[fire] def processAll: Unit = {
    tryWithLog({
      this.process()
      AnnoManager.processAnno(this)
    }) (this.logger, "业务逻辑代码执行完成", "业务逻辑代码执行失败", isThrow = true)
  }

  /**
   * 生命周期方法：用于资源回收与清理，子类复写实现具体逻辑
   * 注：该方法会在进行destroy之前自动被系统调用
   */
  def after(): Unit = {
    AnnoManager.lifeCycleAnno(this, classOf[After])
  }

  /**
   * 生命周期方法：用于回收资源
   */
  def stop(): Unit

  /**
   * 生命周期方法：进行fire框架的资源回收
   */
  protected[fire] def shutdown(stopGracefully: Boolean = true, inListener: Boolean = false): Unit = {
    if (this.isStoped.compareAndSet(false, true)) {
      ThreadUtils.shutdown
      Spark.stop()
      SchedulerManager.shutdown(stopGracefully)
      this.logger.info(s" ${FirePS1Conf.YELLOW}---> 完成fire资源回收 <---${FirePS1Conf.DEFAULT}")
      this.logger.info(s"总耗时：${FirePS1Conf.RED}${elapsed(launchTime)}${FirePS1Conf.DEFAULT} The end...${FirePS1Conf.DEFAULT}")
      if (FireFrameworkConf.shutdownExit) System.exit(0)
    }
  }

  /**
   * 声明周期方法，禁止子类覆写
   */
  final protected def init: Unit = {}

  /**
   * 声明周期方法，禁止子类覆写
   */
  final protected def destory: Unit = {}

  /**
   * 初始化引擎上下文，如SparkSession、StreamExecutionEnvironment等
   * 可根据实际情况，将配置参数放到同名的配置文件中进行差异化的初始化
   */
  def main(args: Array[String]): Unit = {
    this.init(null, args)
  }
}
