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

package com.zto.fire.flink

import com.zto.fire._
import com.zto.fire.common.conf.{FireFrameworkConf, FireHDFSConf, FireHiveConf}
import com.zto.fire.common.util.{OSUtils, PropUtils}
import com.zto.fire.core.BaseFire
import com.zto.fire.core.rest.RestServerManager
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.rest.FlinkSystemRestful
import com.zto.fire.flink.task.FlinkSchedulerManager
import com.zto.fire.flink.util.{FlinkSingletonFactory, FlinkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.hadoop.hive.conf.HiveConf

import scala.util.Try


/**
 * Flink引擎通用父接口
 *
 * @author ChengLong 2020年1月7日 09:31:09
 */
trait BaseFlink extends BaseFire {
  protected[fire] var _conf: Configuration = _
  protected var hiveCatalog: HiveCatalog = _
  protected var parameter: ParameterTool = _

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  override private[fire] def boot: Unit = {
    PropUtils.load(FireFrameworkConf.FLINK_CONF_FILE)
    // flink引擎无需主动在父类中主动加载配置信息，配置加载在GlobalConfiguration中完成
    if (OSUtils.isLocal || FireFrameworkConf.localEnv) {
      this.loadConf
      PropUtils.load(FireFrameworkConf.userCommonConf: _*).loadJobConf(this.getClass.getName)
    }
    PropUtils.setProperty(FireFlinkConf.FLINK_DRIVER_CLASS_NAME, this.className)
    PropUtils.setProperty(FireFlinkConf.FLINK_CLIENT_SIMPLE_CLASS_NAME, this.driverClass)
    FlinkSingletonFactory.setAppName(this.appName)
    super.boot
  }

  /**
   * 初始化flink运行时环境
   */
  override private[fire] def createContext(conf: Any): Unit = {
    if (FlinkUtils.isYarnApplicationMode) {
      // fire rest 服务仅支持flink的yarn-application模式
      this.restfulRegister = new RestServerManager().startRestPort(GlobalConfiguration.getRestPortAndClose)
      this.systemRestful = new FlinkSystemRestful(this, this.restfulRegister)
    }
    PropUtils.show()
    FlinkSchedulerManager.getInstance().registerTasks(this)
    // 创建HiveCatalog
    val metastore = FireHiveConf.getMetastoreUrl
    if (StringUtils.isNotBlank(metastore)) {
      val hiveConf = new HiveConf()
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastore)
      // 根据所选的hive，进行对应hdfs的HA参数设置
      FireHDFSConf.hdfsHAConf.foreach(prop => hiveConf.set(prop._1, prop._2))
      this.hiveCatalog = new HiveCatalog(FireHiveConf.hiveCatalogName, FireHiveConf.defaultDB, hiveConf, FireHiveConf.hiveVersion)
      this.logger.info(s"enabled flink-hive support. catalogName is ${FireHiveConf.hiveCatalogName}")
    }
  }

  /**
   * 构建或合并Configuration
   * 注：不同的子类需根据需要复写该方法
   *
   * @param conf
   * 在conf基础上构建
   * @return
   * 合并后的Configuration对象
   */
  def buildConf(conf: Configuration): Configuration

  /**
   * 生命周期方法：用于回收资源
   */
  override def stop: Unit = {
    try {
      this.after()
    } finally {
      this.shutdown()
    }
  }

  /**
   * 生命周期方法：进行fire框架的资源回收
   * 注：不允许子类覆盖
   */
  override protected[fire] final def shutdown(stopGracefully: Boolean = true, inListener: Boolean = false): Unit = {
    super.shutdown(stopGracefully, inListener)
    if (FireFrameworkConf.shutdownExit) System.exit(0)
  }

  /**
   * 用于解析configuration中的配置，识别flink参数（非用户自定义参数），并设置到env中
   */
  private[fire] def configParse(env: Any): ExecutionConfig = {
    requireNonEmpty(env)("Environment对象不能为空")
    val config = if (env.isInstanceOf[ExecutionEnvironment]) {
      val batchEnv = env.asInstanceOf[ExecutionEnvironment]
      // flink.default.parallelism
      if (FireFlinkConf.defaultParallelism != -1) batchEnv.setParallelism(FireFlinkConf.defaultParallelism)
      batchEnv.getConfig
    } else {
      val streamEnv = env.asInstanceOf[StreamExecutionEnvironment]
      // flink.max.parallelism
      if (FireFlinkConf.maxParallelism != -1) streamEnv.setMaxParallelism(FireFlinkConf.maxParallelism)
      // flink.default.parallelism
      if (FireFlinkConf.defaultParallelism != -1) streamEnv.setParallelism(FireFlinkConf.defaultParallelism)
      // flink.stream.buffer.timeout.millis
      if (FireFlinkConf.streamBufferTimeoutMillis != -1) streamEnv.setBufferTimeout(FireFlinkConf.streamBufferTimeoutMillis)
      // flink.stream.number.execution.retries
      if (FireFlinkConf.streamNumberExecutionRetries != -1) streamEnv.setNumberOfExecutionRetries(FireFlinkConf.streamNumberExecutionRetries)
      // flink.stream.time.characteristic
      if (StringUtils.isNotBlank(FireFlinkConf.streamTimeCharacteristic)) streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(FireFlinkConf.streamTimeCharacteristic))

      // checkPoint相关参数
      val ckConfig = streamEnv.getCheckpointConfig
      if (ckConfig != null && FireFlinkConf.streamCheckpointInterval != -1) {
        // flink.stream.checkpoint.interval 单位：毫秒 默认：-1 关闭
        streamEnv.enableCheckpointing(FireFlinkConf.streamCheckpointInterval)
        // flink.stream.checkpoint.mode  EXACTLY_ONCE/AT_LEAST_ONCE 默认：EXACTLY_ONCE
        if (StringUtils.isNotBlank(FireFlinkConf.streamCheckpointMode)) ckConfig.setCheckpointingMode(CheckpointingMode.valueOf(FireFlinkConf.streamCheckpointMode.trim.toUpperCase))
        // flink.stream.checkpoint.timeout 单位：毫秒 默认：10 * 60 * 1000
        if (FireFlinkConf.streamCheckpointTimeout > 0) ckConfig.setCheckpointTimeout(FireFlinkConf.streamCheckpointTimeout)
        // flink.stream.checkpoint.max.concurrent 默认：1
        if (FireFlinkConf.streamCheckpointMaxConcurrent > 0) ckConfig.setMaxConcurrentCheckpoints(FireFlinkConf.streamCheckpointMaxConcurrent)
        // flink.stream.checkpoint.min.pause.between  默认：-1
        if (FireFlinkConf.streamCheckpointMinPauseBetween >= 0) {
          ckConfig.setMinPauseBetweenCheckpoints(FireFlinkConf.streamCheckpointMinPauseBetween)
        } else {
          // 如果flink.stream.checkpoint.min.pause.between=-1，则默认的checkpoint间隔时间是checkpoint的频率
          ckConfig.setMinPauseBetweenCheckpoints(FireFlinkConf.streamCheckpointInterval)
        }
        // flink.stream.checkpoint.prefer.recovery  默认：false
        // ckConfig.setPreferCheckpointForRecovery(FireFlinkConf.streamCheckpointPreferRecovery)
        // flink.stream.checkpoint.tolerable.failure.number 默认：0
        if (FireFlinkConf.streamCheckpointTolerableFailureNumber >= 0) ckConfig.setTolerableCheckpointFailureNumber(FireFlinkConf.streamCheckpointTolerableFailureNumber)
        // flink.stream.checkpoint.externalized
        if (StringUtils.isNotBlank(FireFlinkConf.streamCheckpointExternalized)) ckConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.valueOf(FireFlinkConf.streamCheckpointExternalized.trim))
        // flink.stream.checkpoint.unaligned.enable
        ckConfig.enableUnalignedCheckpoints(FireFlinkConf.unalignedCheckpointEnable)
      }

      streamEnv.getConfig
    }
    FlinkUtils.parseConf(config)

    config
  }

  /**
   * 获取任务的resourceId
   *
   * @return
   * spark任务：driver/id  flink任务：JobManager/container_xxx
   */
  override protected def resourceId: String = FlinkUtils.getResourceId

  /**
   * SQL语法校验，如果语法错误，则返回错误堆栈
   *
   * @param sql
   * sql statement
   */
  override def sqlValidate(sql: JString): Try[Unit] = FlinkUtils.sqlValidate(sql)

  /**
   * SQL语法校验
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  override def sqlLegal(sql: JString): Boolean = FlinkUtils.sqlLegal(sql)

  /**
   * 初始化引擎上下文，如SparkSession、StreamExecutionEnvironment等
   * 可根据实际情况，将配置参数放到同名的配置文件中进行差异化的初始化
   */
  override def main(args: Array[String]): Unit = {
    try {
      if (args != null && args.nonEmpty) this.parameter = ParameterTool.fromArgs(args)
    } catch {
      case _: Throwable => this.logger.error("ParameterTool 解析main方法参数失败，请注意参数的key必须以-或--开头")
    } finally {
      this.init(null, args)
    }
  }
}
