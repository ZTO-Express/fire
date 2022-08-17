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

import com.zto.fire.common.conf.{FireFrameworkConf, FireHiveConf}
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util.{OSUtils, PropUtils}
import com.zto.fire.flink.util.FlinkSingletonFactory
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * flink batch通用父接口
 * @author ChengLong 2020年1月7日 15:15:56
 */
trait BaseFlinkBatch extends BaseFlink {
  override val jobType: JobType = JobType.FLINK_BATCH
  protected var env, flink, fire: ExecutionEnvironment = _
  protected var tableEnv: TableEnvironment = _

  /**
   * 构建或合并Configuration
   * 注：不同的子类需根据需要复写该方法
   *
   * @param conf
   * 在conf基础上构建
   * @return
   * 合并后的Configuration对象
   */
  override def buildConf(conf: Configuration): Configuration = {
    val finalConf = if (conf != null) conf else {
      val tmpConf = new Configuration()
      PropUtils.settings.foreach(t => tmpConf.setString(t._1, t._2))
      tmpConf
    }
    finalConf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)

    this._conf = finalConf
    finalConf
  }


  /**
   * 程序初始化方法，用于初始化必要的值
   *
   * @param conf
   * 用户指定的配置信息
   * @param args
   * main方法参数列表
   */
  override def init(conf: Any = null, args: Array[String] = null): Unit = {
    super.init(conf, args)
    if (conf != null) conf.asInstanceOf[Configuration].setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)

    this.processAll
  }

  /**
   * 创建计算引擎运行时环境
   *
   * @param conf
   * 配置信息
   */
  override private[fire] def createContext(conf: Any): Unit = {
    super.createContext(conf)
    val finalConf = this.buildConf(conf.asInstanceOf[Configuration])
    if (OSUtils.isLocal) {
      this.env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(finalConf)
    } else {
      this.env = ExecutionEnvironment.getExecutionEnvironment
    }
    this.env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(finalConf.toMap))
    this.configParse(this.env)
    this.tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance.inBatchMode().build())
    if (StringUtils.isNotBlank(FireHiveConf.getHiveConfDir)) {
      this.tableEnv.registerCatalog(FireHiveConf.hiveCatalogName, this.hiveCatalog)
    }
    this.flink = this.env
    this.fire = this.flink
    FlinkSingletonFactory.setEnv(this.env).setTableEnv(this.tableEnv)
  }

  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf(): Unit = {
    // 加载配置文件
    PropUtils.load(FireFrameworkConf.FLINK_BATCH_CONF_FILE)
  }

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    // 子类复写该方法实现业务处理逻辑
  }
}
