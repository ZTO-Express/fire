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

package com.zto.fire.spark

import com.zto.fire._
import com.zto.fire.common.conf.{FireFrameworkConf, FireHDFSConf, FireHiveConf}
import com.zto.fire.common.util.{OSUtils, PropUtils}
import com.zto.fire.core.BaseFire
import com.zto.fire.core.rest.RestServerManager
import com.zto.fire.spark.acc.AccumulatorManager
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.ext.module.KuduContextExt
import com.zto.fire.spark.rest.SparkSystemRestful
import com.zto.fire.spark.task.{SparkInternalTask, SparkSchedulerManager}
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging

/**
 * Spark通用父类
 * Created by ChengLong on 2018-03-06.
 */
trait BaseSpark extends SparkListener with BaseFire with Logging with Serializable {
  private[fire] var _conf: SparkConf = _
  protected[fire] var _spark: SparkSession = _
  protected lazy val spark, fire: SparkSession = _spark
  protected[fire] var sc: SparkContext = _
  protected[fire] var catalog: Catalog = _
  protected[fire] var ssc: StreamingContext = _
  protected[fire] var hiveContext, sqlContext: SQLContext = _
  protected[fire] var kuduContext: KuduContextExt = _
  protected[fire] val acc = AccumulatorManager
  protected[fire] var batchDuration: Long = _
  protected[fire] var listener: SparkListener = _

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  override private[fire] final def boot: Unit = {
    // 进Driver端进行引擎配置与用户配置的加载，executor端会通过fire进行分发，应避免在executor端加载引擎和用户配置文件
    if (SparkUtils.isDriver) {
      this.loadConf
      PropUtils.load(FireFrameworkConf.userCommonConf: _*).load(this.appName)
    }
    PropUtils.setProperty(FireFrameworkConf.DRIVER_CLASS_NAME, this.className)
    if (StringUtils.isNotBlank(FireSparkConf.appName)) {
      this.appName = FireSparkConf.appName
    }
    SparkSingletonFactory.setAppName(this.appName)
    super.boot
    this.logger.info("<-- 完成fire框架初始化 -->")
  }

  /**
   * 生命周期方法：用于关闭SparkContext
   */
  override final def stop: Unit = {
    if (this._spark != null && this.sc != null && !this.sc.isStopped) {
      this._spark.stop()
    }
  }

  /**
   * 生命周期方法：进行fire框架的资源回收
   * 注：不允许子类覆盖
   */
  override protected[fire] final def shutdown(stopGracefully: Boolean = true): Unit = {
    try {
      this.logger.info("<-- 完成用户资源回收 -->")

      if (stopGracefully) {
        if (this.sqlContext != null) this.sqlContext.clearCache
        if (this.ssc != null) {
          this.ssc.stop(true, stopGracefully)
          this.ssc = null
          this.sc = null
        }
        if (this.sc != null && !this.sc.isStopped) {
          this.sc.stop()
          this.sc = null
        }
      }

    } finally {
      super.shutdown(stopGracefully)
    }
  }

  /**
   * 构建或合并SparkConf
   * 注：不同的子类需根据需要复写该方法
   *
   * @param conf
   * 在conf基础上构建
   * @return
   * 合并后的SparkConf对象
   */
  def buildConf(conf: SparkConf): SparkConf = {
    if (conf == null) new SparkConf().setAppName(this.appName) else conf
  }


  /**
   * 构建一系列context对象
   */
  override private[fire] final def createContext(conf: Any): Unit = {
    this.restfulRegister = new RestServerManager().startRestPort()
    this.systemRestful = new SparkSystemRestful(this)
    // 注册到实时平台，并覆盖配置信息
    PropUtils.invokeConfigCenter(this.className)
    PropUtils.show()

    // 构建SparkConf信息
    val tmpConf = if (conf == null) this.buildConf(null) else conf.asInstanceOf[SparkConf]
    tmpConf.setAll(PropUtils.settings)
    tmpConf.set("spark.driver.class.simple.name", this.driverClass)

    // 设置hive metastore地址
    val hiveMetastoreUrl = FireHiveConf.getMetastoreUrl
    if (StringUtils.isBlank(hiveMetastoreUrl)) this.logger.warn("当前任务未指定hive连接信息，将不会连接hive metastore。如需使用hive，请通过spark.hive.cluster=xxx指定。")
    if (StringUtils.isNotBlank(hiveMetastoreUrl)) tmpConf.set("hive.metastore.uris", hiveMetastoreUrl)

    // 构建SparkSession对象
    val sessionBuilder = SparkSession.builder().config(tmpConf)
    if (StringUtils.isNotBlank(hiveMetastoreUrl)) sessionBuilder.enableHiveSupport()
    // 在mac或windows环境下执行local模式，cpu数通过spark.local.cores指定，默认local[*]
    if (OSUtils.isLocal) sessionBuilder.master(s"local[${FireSparkConf.localCores}]")
    this._spark = sessionBuilder.getOrCreate()
    SparkSingletonFactory.setSparkSession(this._spark)
    this._spark.registerUDF()
    this.sc = this._spark.sparkContext
    // 关联所连接的hive集群，根据预制方案启用HDFS HA
    FireHDFSConf.hdfsHAConf.foreach(t => this.sc.hadoopConfiguration.set(t._1, t._2))
    this.catalog = this._spark.catalog
    this.sc.setLogLevel(FireSparkConf.logLevel)
    this.listener = new BaseSparkListener(this)
    this.sc.addSparkListener(listener)
    // this.initLogging(this.className)
    this.hiveContext = this._spark.sqlContext
    this.sqlContext = this.hiveContext
    this.kuduContext = SparkSingletonFactory.getKuduContextInstance(this.sc)
    this.applicationId = SparkUtils.getApplicationId(this._spark)
    this.webUI = SparkUtils.getWebUI(this._spark)
    this._conf = tmpConf
    this.deployConf
    this.logger.info("<-- 完成Spark运行时信息初始化 -->")
    SparkUtils.executeHiveConfSQL(this._spark)
  }

  /**
   * 用于fire框架初始化，传递累加器与配置信息到executor端
   */
  override protected def deployConf: Unit = {
    if (!FireFrameworkConf.deployConf) return
    // 向driver和executor注册定时任务
    val taskSchedule = new SparkInternalTask(this)
    // driver端注册定时任务
    SparkSchedulerManager.getInstance().registerTasks(this, taskSchedule, this.listener)
    // executor端与自定义累加器一同完成定时任务注册
    AccumulatorManager.registerTasks(this, taskSchedule)
    // 向executor端注册自定义累加器
    if (FireFrameworkConf.accEnable) this.acc.registerAccumulators(this.sc)
  }

  /**
   * 用于注册定时任务实例
   *
   * @param instances
   * 标记有@Scheduled类的实例
   */
  def registerSchedule(instances: Object*): Unit = {
    try {
      // 向driver端注册定时任务
      SparkSchedulerManager.getInstance().registerTasks(instances: _*)
      // 向executor端注册定时任务
      val executors = this._conf.get("spark.executor.instances").toInt
      if (executors > 0 && this.sc != null) {
        this.sc.parallelize(1 to executors, executors).foreachPartition(i => SparkSchedulerManager.getInstance().registerTasks(instances: _*))
      }
    } catch {
      case e: Throwable => this.logger.error("定时任务注册失败.", e)
    }
  }
}