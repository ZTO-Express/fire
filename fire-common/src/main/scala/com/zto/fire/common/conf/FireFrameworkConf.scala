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
import org.apache.commons.lang3.StringUtils

/**
 * Fire框架相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:54
 */
private[fire] object FireFrameworkConf {
  // fire版本号
  lazy val FIRE_VERSION = "fire.version"
  lazy val DRIVER_CLASS_NAME = "driver.class.name"
  // fire内置线程池大小
  lazy val FIRE_THREAD_POOL_SIZE = "fire.thread.pool.size"
  // fire内置定时任务线程池大小
  lazy val FIRE_THREAD_POOL_SCHEDULE_SIZE = "fire.thread.pool.schedule.size"
  // 是否启用fire框架restful服务
  lazy val FIRE_REST_ENABLE = "fire.rest.enable"
  lazy val FIRE_REST_URL_HOSTNAME = "fire.rest.url.hostname"
  lazy val FIRE_CONF_DEPLOY_ENGINE = "fire.conf.deploy.engine"
  lazy val FIRE_ENGINE_CONF_HELPER = "com.zto.fire.core.conf.EngineConfHelper"
  // rest接口权限认证
  lazy val FIRE_REST_FILTER_ENABLE = "fire.rest.filter.enable"
  // 用于配置是否关闭fire内置的所有累加器
  lazy val FIRE_ACC_ENABLE = "fire.acc.enable"
  // 日志累加器开关
  lazy val FIRE_ACC_LOG_ENABLE = "fire.acc.log.enable"
  // 多值累加器开关
  lazy val FIRE_ACC_MULTI_COUNTER_ENABLE = "fire.acc.multi.counter.enable"
  // 多时间维度累加器开关
  lazy val FIRE_ACC_MULTI_TIMER_ENABLE = "fire.acc.multi.timer.enable"
  // env累加器开关
  lazy val FIRE_ACC_ENV_ENABLE = "fire.acc.env.enable"
  // fire框架埋点日志开关，当关闭后，埋点的日志将不再被记录到日志累加器中，并且也不再打印
  lazy val FIRE_LOG_ENABLE = "fire.log.enable"
  // 用于限定fire框架中sql日志的字符串长度
  lazy val FIRE_LOG_SQL_LENGTH = "fire.log.sql.length"
  // fire框架rest接口服务最大线程数
  lazy val FIRE_RESTFUL_MAX_THREAD = "fire.restful.max.thread"
  lazy val FIRE_CONNECTOR_SHUTDOWN_HOOK_ENABLE = "fire.connector.shutdown_hook.enable"
  // 用于配置是否抛弃配置中心独立运行
  lazy val FIRE_CONFIG_CENTER_ENABLE = "fire.config_center.enable"
  // 本地运行环境下（Windows、Mac）是否调用配置中心接口获取配置信息
  lazy val FIRE_CONFIG_CENTER_LOCAL_ENABLE = "fire.config_center.local.enable"
  // 配置中心接口调用秘钥
  lazy val FIRE_CONFIG_CENTER_SECRET = "fire.config_center.register.conf.secret"
  // fire框架restful端口冲突重试次数
  lazy val FIRE_RESTFUL_PORT_RETRY_NUM = "fire.restful.port.retry_num"
  // fire框架restful端口冲突重试时间（ms）
  lazy val FIRE_RESTFUL_PORT_RETRY_DURATION = "fire.restful.port.retry_duration"
  lazy val FIRE_REST_SERVER_SECRET = "fire.rest.server.secret"
  lazy val FIRE_LOG_LEVEL_CONF_PREFIX = "fire.log.level.conf."
  lazy val FIRE_USER_COMMON_CONF = "fire.user.common.conf"
  // 日志记录器保留最少的记录数
  lazy val FIRE_ACC_LOG_MIN_SIZE = "fire.acc.log.min.size"
  // 日志记录器保留最多的记录数
  lazy val FIRE_ACC_LOG_MAX_SIZE = "fire.acc.log.max.size"
  // env累加器保留最多的记录数
  lazy val FIRE_ACC_ENV_MAX_SIZE = "fire.acc.env.max.size"
  // env累加器保留最少的记录数
  lazy val FIRE_ACC_ENV_MIN_SIZE = "fire.acc.env.min.size"
  // timer累加器保留最大的记录数
  lazy val FIRE_ACC_TIMER_MAX_SIZE = "fire.acc.timer.max.size"
  // timer累加器清理几小时之前的记录
  lazy val FIRE_ACC_TIMER_MAX_HOUR = "fire.acc.timer.max.hour"
  // 定时调度任务黑名单（定时任务方法名），以逗号分隔
  lazy val FIRE_SCHEDULER_BLACKLIST = "fire.scheduler.blacklist"
  // 用于配置是否启用任务定时调度
  lazy val FIRE_TASK_SCHEDULE_ENABLE = "fire.task.schedule.enable"
  // quartz最大线程池大小
  lazy val FIRE_QUARTZ_MAX_THREAD = "fire.quartz.max.thread"
  // fire框架restful地址
  lazy val FIRE_REST_URL = "fire.rest.url"
  lazy val FIRE_SHUTDOWN_EXIT = "fire.shutdown.auto.exit"
  // 配置中心生产环境注册地址
  lazy val FIRE_CONFIG_CENTER_REGISTER_CONF_PROD_ADDRESS = "fire.config_center.register.conf.prod.address"
  // 配置中心测试环境注册地址
  lazy val FIRE_CONFIG_CENTER_REGISTER_CONF_TEST_ADDRESS = "fire.config_center.register.conf.test.address"
  // 配置打印黑名单，配置项以逗号分隔
  lazy val FIRE_CONF_PRINT_BLACKLIST = "fire.conf.print.blacklist"
  // 是否启用动态配置功能
  lazy val FIRE_DYNAMIC_CONF_ENABLE = "fire.dynamic.conf.enable"
  // 是否打印配置信息
  lazy val FIRE_CONF_SHOW_ENABLE = "fire.conf.show.enable"
  // 是否将fire restful地址以日志形式打印
  lazy val FIRE_REST_URL_SHOW_ENABLE = "fire.rest.url.show.enable"
  lazy val SPARK_STREAMING_CONF_FILE = "spark-streaming"
  lazy val SPARK_STRUCTURED_STREAMING_CONF_FILE = "structured-streaming"
  lazy val SPARK_CORE_CONF_FILE = "spark-core"
  lazy val FLINK_CONF_FILE = "flink"
  lazy val FLINK_STREAMING_CONF_FILE = "flink-streaming"
  lazy val FLINK_BATCH_CONF_FILE = "flink-batch"
  lazy val FIRE_DEPLOY_CONF_ENABLE = "fire.deploy_conf.enable"
  lazy val FIRE_EXCEPTION_BUS_SIZE = "fire.exception_bus.size"
  lazy val FIRE_BURIED_POINT_DATASOURCE_ENABLE = "fire.buried_point.datasource.enable"
  lazy val FIRE_BURIED_POINT_DATASOURCE_MAX_SIZE = "fire.buried_point.datasource.max.size"
  lazy val FIRE_BURIED_POINT_DATASOURCE_INITIAL_DELAY = "fire.buried_point.datasource.initialDelay"
  lazy val FIRE_BURIED_POINT_DATASOURCE_PERIOD = "fire.buried_point.datasource.period"
  lazy val FIRE_BURIED_POINT_DATASOURCE_MAP = "fire.buried_point.datasource.map."
  lazy val FIRE_CONF_ADAPTIVE_PREFIX = "fire.conf.adaptive.prefix"

  /**
   * 用于jdbc url的识别，当无法通过driver class识别数据源时，将从url中的端口号进行区分
   * 不同数据配置使用统一的前缀：fire.buried_point.datasource.map.
   */
  def buriedPointDatasourceMap: Map[String, String] = PropUtils.sliceKeys(this.FIRE_BURIED_POINT_DATASOURCE_MAP)
  // 获取当前任务的rest server访问地址
  lazy val fireRestUrl = PropUtils.getString(this.FIRE_REST_URL, "")
  // 是否启用hostname作为fire rest url
  lazy val restUrlHostname = PropUtils.getBoolean(this.FIRE_REST_URL_HOSTNAME, false)
  // 不同引擎配置获取具体的实现
  lazy val confDeployEngine = PropUtils.getString(this.FIRE_CONF_DEPLOY_ENGINE, "")
  // 定时解析埋点SQL的执行频率（s）
  lazy val buriedPointDatasourcePeriod = PropUtils.getInt(this.FIRE_BURIED_POINT_DATASOURCE_PERIOD, 60)
  // 定时解析埋点SQL的初始延迟（s）
  lazy val buriedPointDatasourceInitialDelay = PropUtils.getInt(this.FIRE_BURIED_POINT_DATASOURCE_INITIAL_DELAY, 30)
  // 用于存放埋点的队列最大大小，超过该大小将会被丢弃
  lazy val buriedPointDatasourceMaxSize = PropUtils.getInt(this.FIRE_BURIED_POINT_DATASOURCE_MAX_SIZE, 300)
  // 是否开启数据源埋点
  lazy val buriedPointDatasourceEnable = PropUtils.getBoolean(this.FIRE_BURIED_POINT_DATASOURCE_ENABLE, true)
  // 每个jvm实例内部queue用于存放异常对象数最大大小，避免队列过大造成内存溢出
  lazy val exceptionBusSize = PropUtils.getInt(this.FIRE_EXCEPTION_BUS_SIZE, 1000)
  // 是否将配置同步到executor、taskmanager端
  lazy val deployConf = PropUtils.getBoolean(this.FIRE_DEPLOY_CONF_ENABLE, true)
  // fire内置线程池大小
  lazy val threadPoolSize = PropUtils.getInt(this.FIRE_THREAD_POOL_SIZE, 5)
  // fire内置定时任务线程池大小
  lazy val threadPoolSchedulerSize = PropUtils.getInt(this.FIRE_THREAD_POOL_SCHEDULE_SIZE, 5)
  // 自适应前缀，调用getOriginalProperty避免栈溢出
  lazy val adaptivePrefix = PropUtils.getOriginalProperty(this.FIRE_CONF_ADAPTIVE_PREFIX).toBoolean
  // 用户公共配置文件列表
  lazy val userCommonConf = PropUtils.getString(this.FIRE_USER_COMMON_CONF, "").split(",").map(conf => conf.trim).toList
  // fire接口认证秘钥
  lazy val restServerSecret = PropUtils.getString(this.FIRE_REST_SERVER_SECRET)
  // 用于配置是否在调用shutdown后主动退出jvm进程
  lazy val shutdownExit = PropUtils.getBoolean(this.FIRE_SHUTDOWN_EXIT, false)
  // 是否启用为connector注册shutdown hook，当jvm退出前close
  lazy val connectorShutdownHookEnable = PropUtils.getBoolean(this.FIRE_CONNECTOR_SHUTDOWN_HOOK_ENABLE, false)

  // fire日志打印黑名单
  lazy val fireConfBlackList: Set[String] = {
    val blacklist = PropUtils.getString(this.FIRE_CONF_PRINT_BLACKLIST, "")
    if (StringUtils.isNotBlank(blacklist)) blacklist.split(",").toSet else Set.empty
  }

  // 获取driver的class name
  lazy val driverClassName = PropUtils.getString(this.DRIVER_CLASS_NAME)
  // 是否打印配置信息
  lazy val fireConfShow: Boolean = PropUtils.getBoolean(this.FIRE_CONF_SHOW_ENABLE, true)
  // 是否将restful地址以日志方式打印
  lazy val fireRestUrlShow: Boolean = PropUtils.getBoolean(this.FIRE_REST_URL_SHOW_ENABLE, false)
  // 获取动态配置参数
  lazy val dynamicConf: Boolean = PropUtils.getBoolean(this.FIRE_DYNAMIC_CONF_ENABLE, true)

  // 用于获取fire版本号
  lazy val fireVersion = PropUtils.getString(this.FIRE_VERSION, "1.0.0")
  // quartz最大线程池大小
  lazy val quartzMaxThread = PropUtils.getString(this.FIRE_QUARTZ_MAX_THREAD, "8")
  // 用于设置是否启用任务定时调度
  lazy val scheduleEnable = PropUtils.getBoolean(this.FIRE_TASK_SCHEDULE_ENABLE, true)
  // 定时任务黑名单，配置的value为方法名，多个以逗号分隔
  def schedulerBlackList: String = PropUtils.getString(this.FIRE_SCHEDULER_BLACKLIST, "")
  // env累加器开关
  lazy val accEnvEnable = PropUtils.getBoolean(this.FIRE_ACC_ENV_ENABLE, true)
  // 是否启用Fire内置的restful服务
  lazy val restEnable = PropUtils.getBoolean(this.FIRE_REST_ENABLE, true)
  // rest接口权限认证
  lazy val restFilter = PropUtils.getBoolean(this.FIRE_REST_FILTER_ENABLE, true)
  // 是否关闭fire内置的所有累加器
  lazy val accEnable = PropUtils.getBoolean(this.FIRE_ACC_ENABLE, true)
  // 日志累加器开关
  lazy val accLogEnable = PropUtils.getBoolean(this.FIRE_ACC_LOG_ENABLE, true)
  // 多值累加器开关
  lazy val accMultiCounterEnable = PropUtils.getBoolean(this.FIRE_ACC_MULTI_COUNTER_ENABLE, true)
  // 多时间维度累加器开关
  lazy val accMultiTimerEnable = PropUtils.getBoolean(this.FIRE_ACC_MULTI_TIMER_ENABLE, true)
  // fire框架埋点日志开关
  lazy val logEnable = PropUtils.getBoolean(this.FIRE_LOG_ENABLE, true)
  // 用于限定fire框架中sql日志的字符串长度
  lazy val logSqlLength = PropUtils.getInt(this.FIRE_LOG_SQL_LENGTH, 50)
  // 配置中心生产环境注册地址
  lazy val configCenterProdAddress = PropUtils.getString(this.FIRE_CONFIG_CENTER_REGISTER_CONF_PROD_ADDRESS, "")
  // 配置中心测试环境注册地址
  lazy val configCenterTestAddress = PropUtils.getString(this.FIRE_CONFIG_CENTER_REGISTER_CONF_TEST_ADDRESS)


  // fire框架rest接口服务最大线程数
  lazy val restfulMaxThread = PropUtils.getInt(this.FIRE_RESTFUL_MAX_THREAD, 8)
  // 用于配置是否抛弃配置中心独立运行
  lazy val configCenterEnable = PropUtils.getBoolean(this.FIRE_CONFIG_CENTER_ENABLE, true)
  // 本地运行环境下（Windows、Mac）是否调用配置中心接口获取配置信息
  lazy val configCenterLocalEnable = PropUtils.getBoolean(this.FIRE_CONFIG_CENTER_LOCAL_ENABLE, false)
  // 配置中心接口调用秘钥
  lazy val configCenterSecret = PropUtils.getString(this.FIRE_CONFIG_CENTER_SECRET, "")
  // fire框架restful端口冲突重试次数
  lazy val restfulPortRetryNum = PropUtils.getInt(this.FIRE_RESTFUL_PORT_RETRY_NUM, 3)
  // fire框架restful端口冲突重试时间（ms）
  lazy val restfulPortRetryDuration = PropUtils.getLong(this.FIRE_RESTFUL_PORT_RETRY_DURATION, 1000L)
  // 用于限定日志最少保存量，防止当日志量达到maxLogSize时频繁的进行clear操作
  lazy val minLogSize = PropUtils.getInt(this.FIRE_ACC_LOG_MIN_SIZE, 500).abs
  // 用于限定日志最大保存量，防止日志量过大，撑爆driver
  lazy val maxLogSize = PropUtils.getInt(this.FIRE_ACC_LOG_MAX_SIZE, 1000).abs
  // 用于限定运行时信息最少保存量，防止当运行时信息量达到maxEnvSize时频繁的进行clear操作
  lazy val minEnvSize = PropUtils.getInt(this.FIRE_ACC_ENV_MIN_SIZE, 100).abs
  // 用于限定运行时信息最大保存量，防止过大撑爆driver
  lazy val maxEnvSize = PropUtils.getInt(this.FIRE_ACC_ENV_MAX_SIZE, 500).abs
  // 用于限定最大保存量，防止数据量过大，撑爆driver
  lazy val maxTimerSize = PropUtils.getInt(this.FIRE_ACC_TIMER_MAX_SIZE, 1000).abs
  // 用于指定清理指定小时数之前的记录
  lazy val maxTimerHour = PropUtils.getInt(this.FIRE_ACC_TIMER_MAX_HOUR, 12).abs
}