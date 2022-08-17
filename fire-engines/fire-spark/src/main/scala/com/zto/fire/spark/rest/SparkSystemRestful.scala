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

package com.zto.fire.spark.rest

import com.google.common.collect.Table
import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.{ErrorCode, RequestMethod}
import com.zto.fire.common.util.{ExceptionBus, _}
import com.zto.fire.core.rest.{RestCase, SystemRestful}
import com.zto.fire.spark.{BaseSpark, bean}
import org.apache.commons.lang3.StringUtils
import spark._
import com.zto.fire._
import com.zto.fire.core.bean.ArthasParam
import com.zto.fire.spark.bean.{ColumnMeta, FunctionMeta, SparkInfo}
import com.zto.fire.spark.plugin.SparkArthasLauncher
import com.zto.fire.spark.sync.SyncSparkEngineConf

import java.util


/**
 * 系统预定义的restful服务，为Spark计算引擎提供接口服务
 *
 * @author ChengLong 2019-3-16 10:16:38
 */
private[fire] class SparkSystemRestful(val baseSpark: BaseSpark) extends SystemRestful(baseSpark) {
  private var sparkInfoBean: SparkInfo = _

  /**
   * 注册Spark引擎接口
   */
  override def register: Unit = {
    this.baseSpark.restfulRegister
      .addRest(RestCase(RequestMethod.DELETE.toString, s"/system/kill", kill))
      .addRest(RestCase(RequestMethod.DELETE.toString, s"/system/cancelJob", cancelJob))
      .addRest(RestCase(RequestMethod.DELETE.toString, s"/system/cancelStage", cancelStage))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/sql", sql))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/sparkInfo", sparkInfo))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/counter", counter))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/multiCounter", multiCounter))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/multiTimer", multiTimer))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/log", log))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/env", env))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/listDatabases", listDatabases))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/listTables", listTables))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/listColumns", listColumns))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/listFunctions", listFunctions))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/setConf", setConf))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/datasource", datasource))
      .addRest(RestCase(RequestMethod.POST.toString, s"/system/arthas", arthas))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/exception", exception))
  }

  /**
   * 用于更新配置信息
   */
  @Rest("/system/setConf")
  def setConf(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/setConf")
      this.logger.info(s"请求fire更新配置信息：$json")
      val confMap = JSONUtils.parseObject[java.util.HashMap[String, String]](json)
      if (ValueUtils.noEmpty(confMap)) {
        PropUtils.setProperties(confMap)
        this.baseSpark._conf.setAll(PropUtils.settings)
        SyncSparkEngineConf.syncDynamicConf(this.baseSpark.sc, this.baseSpark._conf)
      }
      ResultMsg.buildSuccess("配置信息已更新", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[setConf] 设置配置信息失败：json=$json", e)
        ResultMsg.buildError("设置配置信息失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 根据函数信息
   */
  @Rest("/system/listFunctions")
  def listFunctions(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/listFunctions")
      // 参数合法性检查
      val dbName = JSONUtils.getValue(json, "dbName", "")

      // 获取已注册的函数
      val funList = new util.LinkedList[FunctionMeta]()
      if (StringUtils.isNotBlank(dbName)) {
        this.baseSpark.catalog.listFunctions(dbName).collect().foreach(fun => {
          funList.add(new FunctionMeta(fun.description, fun.database, fun.name, fun.className, fun.isTemporary))
        })
      } else {
        this.baseSpark.catalog.listFunctions().collect().foreach(fun => {
          funList.add(new FunctionMeta(fun.description, fun.database, fun.name, fun.className, fun.isTemporary))
        })
      }
      this.logger.info(s"[listFunctions] 获取[$dbName]函数信息成功：json=$json")
      ResultMsg.buildSuccess(funList, s"获取[$dbName]函数信息成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取函数信息失败：json=$json", e)
        ResultMsg.buildError("获取函数信息失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 根据表名获取字段信息
   */
  @Rest("/system/listColumns")
  def listColumns(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/listColumns")
      // 参数合法性检查
      val dbName = JSONUtils.getValue(json, "dbName", "memory")
      val tableName = JSONUtils.getValue(json, "tableName", "")
      if (StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName)) {
        return ResultMsg.buildError("获取表元字段信息失败，库名和表名不能为空", ErrorCode.PARAM_ILLEGAL)
      }

      // 区分内存临时表和物理表
      val columns = if ("memory".equals(dbName)) {
        this.baseSpark.catalog.listColumns(tableName)
      } else {
        this.baseSpark.catalog.listColumns(dbName, tableName)
      }

      // 将字段元数据信息封装
      val columnList = new util.LinkedList[ColumnMeta]
      columns.collect().foreach(column => {
        val meta = new ColumnMeta.Builder().setColumnName(column.name)
          .setBucket(column.isBucket)
          .setDatabase(dbName)
          .setDataType(column.dataType)
          .setTableName(tableName)
          .setDescription(column.description)
          .setNullable(column.nullable)
          .setPartition(column.isPartition).build()
        columnList.add(meta)
      })

      this.logger.info(s"[listColumns] 获取[$dbName.$tableName]字段信息成功：json=$json")
      ResultMsg.buildSuccess(columnList, s"获取[$dbName.$tableName]字段信息成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取表字段信息失败：json=$json", e)
        ResultMsg.buildError("获取表字段信息失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取指定数据库下所有的表信息
   */
  @Rest("/system/listTables")
  def listTables(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/listTables")
      // 参数合法性检查
      val dbName = JSONUtils.getValue(json, "dbName", "memory")
      if (StringUtils.isBlank(dbName)) {
        return ResultMsg.buildError("获取表元数据信息失败，库名不能为空", ErrorCode.PARAM_ILLEGAL)
      }

      val tableList = new util.LinkedList[bean.TableMeta]
      if ("memory".equals(dbName)) {
        // 内存临时表元数据信息
        this.baseSpark.catalog.listTables().collect().foreach(table => {
          if (StringUtils.isBlank(table.database)) {
            tableList.add(new bean.TableMeta(table.description, "memory", table.name, table.tableType, table.isTemporary))
          }
        })
      } else {
        // 获取hive表元数据信息
        this.baseSpark.catalog.listTables(dbName).collect().foreach(table => {
          if (StringUtils.isNotBlank(table.database)) {
            tableList.add(new bean.TableMeta(table.description, table.database, table.name, table.tableType, table.isTemporary))
          }
        })
      }
      this.logger.info(s"[listTables] 获取[$dbName]表元数据信息成功：json=$json")
      ResultMsg.buildSuccess(tableList, s"获取[$dbName]表元数据信息成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取表元数据信息失败：json=$json", e)
        ResultMsg.buildError("获取表元数据信息失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取数据库列表
   */
  @Rest("/system/listDatabases")
  def listDatabases(request: Request, response: Response): AnyRef = {
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/listDatabases")
      // 获取所有的数据库名称
      val dbList = new util.LinkedList[String]()
      this.baseSpark.catalog.listDatabases().collect().foreach(db => dbList.add(db.name))
      // 由于spark临时表没有库名，此处约定memory统一作为临时表所在的库
      dbList.add("memory")

      this.logger.info(s"[listDatabases] 获取数据库列表成功")
      ResultMsg.buildSuccess(dbList, "获取数据库列表成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取数据库列表失败", e)
        ResultMsg.buildError("获取数据库列表失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取counter累加器中的值
   */
  @Rest("/system/counter")
  def counter(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/counter")
      val counter = this.baseSpark.acc.getCounter
      this.logger.info(s"[counter] 获取单值累加器成功：counter=$counter")
      ResultMsg.buildSuccess(counter, "获取单值累加器成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取单值累加器失败：json=$json", e)
        ResultMsg.buildError("获取多值累加器失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取多值累加器中的值
   */
  @Rest("/system/multiCounter")
  def multiCounter(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/multiCounter")
      this.logger.info(s"[multiCounter] 获取多值累加器成功")
      ResultMsg.buildSuccess(this.baseSpark.acc.getMultiCounter, "获取多值累加器成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取多值累加器失败：json=$json", e)
        ResultMsg.buildError("获取多值累加器失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取timer累加器中的值
   */
  @Rest("/system/multiTimer")
  def multiTimer(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/multiTimer")
      val cells = new util.HashSet[Table.Cell[String, String, Long]]()
      cells.addAll(this.baseSpark.acc.getMultiTimer.cellSet())
      val clear = JSONUtils.getValue(json, "clear", false)

      if (clear) this.baseSpark.acc.multiTimer.reset
      this.logger.info(s"[multiTimer] 获取timer累加器成功")

      ResultMsg.buildSuccess(cells, "获取timer累加器成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取timer累加器失败：json=$json", e)
        ResultMsg.buildError("获取timer累加器失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取运行时日志
   */
  @Rest("/system/log")
  def log(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/log")
      val logs = new StringBuilder("[")
      this.baseSpark.acc.getLog.iterator().foreach(log => {
        logs.append(log + ",")
      })

      // 参数校验与参数获取
      val clear = JSONUtils.getValue(json, "clear", false)
      if (clear) this.baseSpark.acc.logAccumulator.reset

      if (logs.length > 0 && logs.endsWith(",")) {
        this.logger.info(s"[log] 日志获取成功：json=$json")
        ResultMsg.buildSuccess(logs.substring(0, logs.length - 1) + "]", "日志获取成功")
      } else {
        this.logger.info(s"[log] 日志记录数为空：json=$json")
        ResultMsg.buildError("日志记录数为空", ErrorCode.NOT_FOUND)
      }
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 日志获取失败：json=$json", e)
        ResultMsg.buildError("日志获取失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取运行时状态信息，包括GC、jvm、thread、memory、cpu等
   */
  @Rest("/system/env")
  def env(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/env")
      val envInfo = new StringBuilder("[")
      this.baseSpark.acc.getEnv.iterator().foreach(env => {
        envInfo.append(env + ",")
      })

      // 参数校验与参数获取
      val clear = JSONUtils.getValue(json, "clear", false)
      if (clear) this.baseSpark.acc.logAccumulator.reset

      if (envInfo.length > 0 && envInfo.endsWith(",")) {
        this.logger.info(s"[env] 运行时信息获取成功：json=$json")
        ResultMsg.buildSuccess(envInfo.substring(0, envInfo.length - 1) + "]", "运行时信息获取成功")
      } else {
        this.logger.info(s"[env] 运行时信息记录数为空：json=$json")
        ResultMsg.buildError("运行时信息记录数为空", ErrorCode.NOT_FOUND)
      }
    } catch {
      case e: Exception => {
        this.logger.error(s"[env] 运行时信息获取失败：json=$json", e)
        ResultMsg.buildError("运行时信息获取失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * kill 当前 Spark 任务
   */
  @Rest("/system/kill")
  def kill(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/kill")
      // 参数校验与参数获取
      val stopGracefully = JSONUtils.getValue(json, "stopGracefully", true)
      this.baseSpark.after()
      this.baseSpark.shutdown(stopGracefully)
      ProcessUtil.executeCmds(s"yarn application -kill ${this.baseSpark.applicationId}", s"kill -9 ${OSUtils.getPid}")
      this.logger.info(s"[kill] kill任务成功：json=$json")
      System.exit(0)
      ResultMsg.buildSuccess("任务停止成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[kill] 执行kill任务失败：json=$json", e)
        ResultMsg.buildError("执行kill任务失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 取消job的执行
   */
  @Rest("/system/cancelJob")
  def cancelJob(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/cancelJob")
      // 参数校验与参数获取
      val jobId = JSONUtils.getValue(json, "id", -1)
      if (jobId <= 0) {
        this.logger.warn(s"[cancelJob] 参数不合法：json=$json")
        return ResultMsg.buildError(s"参数不合法：json=$json", ErrorCode.ERROR)
      }

      this.baseSpark.sc.cancelJob(jobId, s"被管控平台kill：${DateFormatUtils.formatCurrentDateTime()}")
      this.logger.info(s"[cancelJob] kill job成功：json=$json")
      ResultMsg.buildSuccess("kill job 成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[cancelJob] kill job失败：json=$json", e)
        ResultMsg.buildError("kill job失败", ErrorCode.ERROR)
      }
    }
  }

  /**
   * 取消stage的执行
   */
  @Rest("/system/cancelStage")
  def cancelStage(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/cancelStage")
      // 参数校验与参数获取
      val stageId = JSONUtils.getValue(json, "id", -1)
      if (stageId <= 0) {
        this.logger.warn(s"[cancelStage] 参数不合法：json=$json")
        return ResultMsg.buildError(s"参数不合法：json=$json", ErrorCode.ERROR)
      }

      this.baseSpark.sc.cancelStage(stageId, s"被管控平台kill：${DateFormatUtils.formatCurrentDateTime()}")
      this.logger.info(s"[cancelStage] kill stage[${stageId}] 成功：json=$json")
      ResultMsg.buildSuccess("kill stage 成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[cancelStage] kill stage失败：json=$json", e)
        ResultMsg.buildError("kill stage失败", ErrorCode.ERROR)
      }
    }
  }


  /**
   * 用于执行sql语句
   */
  @Rest(value = "/system/sql", method = "post")
  def sql(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/sql")
      // 参数校验与参数获取
      val sql = JSONUtils.getValue(json, "sql", "")

      // sql合法性检查
      if (StringUtils.isBlank(sql) || !sql.toLowerCase.trim.startsWith("select ")) {
        this.logger.warn(s"[sql] sql不合法，在线调试功能只支持查询操作：json=$json")
        return ResultMsg.buildError(s"sql不合法，在线调试功能只支持查询操作", ErrorCode.ERROR)
      }

      if (this.baseSpark == null || this.baseSpark._spark == null) {
        this.logger.warn(s"[sql] 系统正在初始化，请稍后再试：json=$json")
        return "系统正在初始化，请稍后再试"
      }

      val sqlResult = this.baseSpark._spark.sql(sql.replace("memory.", "")).limit(1000).showString()
      this.logger.info(s"成功执行以下查询：${sql}\n执行结果如下：\n" + sqlResult)
      ResultMsg.buildSuccess(sqlResult, ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[sql] 执行用户sql失败：json=$json", e)
        ResultMsg.buildError("执行用户sql失败，异常堆栈：" + ExceptionBus.stackTrace(e), ErrorCode.ERROR)
      }
    }
  }

  /**
   * 获取当前的spark运行时信息
   */
  @Rest("/system/sparkInfo")
  def sparkInfo(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/sparkInfo")
      if (this.sparkInfoBean == null) {
        this.sparkInfoBean = new SparkInfo
        this.sparkInfoBean.setAppName(this.baseSpark.appName)
        this.sparkInfoBean.setClassName(this.baseSpark.className)
        this.sparkInfoBean.setFireVersion(FireFrameworkConf.fireVersion)
        this.sparkInfoBean.setConf(this.baseSpark._spark.conf.getAll)
        this.sparkInfoBean.setVersion(this.baseSpark.sc.version)
        this.sparkInfoBean.setMaster(this.baseSpark.sc.master)
        this.sparkInfoBean.setApplicationId(this.baseSpark.sc.applicationId)
        this.sparkInfoBean.setApplicationAttemptId(this.baseSpark.sc.applicationAttemptId.getOrElse(""))
        this.sparkInfoBean.setUi(this.baseSpark.webUI)
        this.sparkInfoBean.setPid(OSUtils.getPid)
        this.sparkInfoBean.setStartTime(DateFormatUtils.formatUnixDateTime(this.baseSpark.startTime * 1000))
        this.sparkInfoBean.setExecutorMemory(this.baseSpark.sc.getConf.get("spark.executor.memory", "1"))
        this.sparkInfoBean.setExecutorInstances(this.baseSpark.sc.getConf.get("spark.executor.instances", "1"))
        this.sparkInfoBean.setExecutorCores(this.baseSpark.sc.getConf.get("spark.executor.cores", "1"))
        this.sparkInfoBean.setDriverCores(this.baseSpark.sc.getConf.get("spark.driver.cores", "1"))
        this.sparkInfoBean.setDriverMemory(this.baseSpark.sc.getConf.get("spark.driver.memory", "1"))
        this.sparkInfoBean.setDriverMemoryOverhead(this.baseSpark.sc.getConf.get("spark.yarn.driver.memoryOverhead", "0"))
        this.sparkInfoBean.setDriverHost(this.baseSpark.sc.getConf.get("spark.driver.host", "0"))
        this.sparkInfoBean.setDriverPort(this.baseSpark.sc.getConf.get("spark.driver.port", "0"))
        this.sparkInfoBean.setRestPort(this.baseSpark.restfulRegister.restPort.toString)
        this.sparkInfoBean.setExecutorMemoryOverhead(this.baseSpark.sc.getConf.get("spark.yarn.executor.memoryOverhead", "0"))
        this.sparkInfoBean.setProperties(PropUtils.cover)
        this.sparkInfoBean.computeCpuMemory()
      }
      this.sparkInfoBean.setUptime(DateFormatUtils.runTime(this.baseSpark.startTime))
      this.sparkInfoBean.setBatchDuration(this.baseSpark.batchDuration + "")
      this.sparkInfoBean.setTimestamp(DateFormatUtils.formatCurrentDateTime())
      this.logger.info(s"[sparkInfo] 获取spark信息成功：json=$json")
      ResultMsg.buildSuccess(JSONUtils.toJSONString(this.sparkInfoBean), ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[sparkInfo] 获取spark信息失败：json=$json", e)
        ResultMsg.buildError("获取spark信息失败", ErrorCode.ERROR)
      }
    }
  }

}
