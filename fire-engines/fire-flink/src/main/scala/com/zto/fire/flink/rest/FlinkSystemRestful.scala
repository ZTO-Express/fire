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

package com.zto.fire.flink.rest

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.enu.{ErrorCode, RequestMethod}
import com.zto.fire.common.util.{ExceptionBus, _}
import com.zto.fire.core.rest.{RestCase, RestServerManager, SystemRestful}
import com.zto.fire.flink.BaseFlink
import org.apache.commons.lang3.StringUtils
import spark._

/**
 * 系统预定义的restful服务，为Flink计算引擎提供接口服务
 *
 * @author ChengLong 2020年4月2日 13:50:01
 */
private[fire] class FlinkSystemRestful(var baseFlink: BaseFlink, val restfulRegister: RestServerManager) extends SystemRestful(baseFlink) {

  /**
   * 注册Flink引擎restful接口
   */
  override protected def register: Unit = {
    this.restfulRegister
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/flink/kill", kill))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/flink/datasource", datasource))
  }

  /**
   * 设置baseFlink实例
   */
  private[fire] def setBaseFlink(baseFlink: BaseFlink): Unit = this.baseFlink = baseFlink

  /**
   * kill 当前 Flink 任务
   */
  @Rest("/system/flink/kill")
  def kill(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg
    val json = request.body
    try {
      // 参数校验与参数获取
      this.baseFlink.shutdown()
      this.logger.info(s"[kill] kill任务成功：json=$json")
      msg.buildSuccess("任务停止成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[kill] 执行kill任务失败：json=$json", e)
        msg.buildError("执行kill任务失败", ErrorCode.ERROR)
      }
    }
  }


  /**
   * 用于执行sql语句
   */
  @Rest(value = "/system/sql", method = "post")
  def sql(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg
    val json = request.body
    try {
      // 参数校验与参数获取
      val sql = JSONUtils.getValue(json, "sql", "")

      // sql合法性检查
      if (StringUtils.isBlank(sql) || !sql.toLowerCase.trim.startsWith("select ")) {
        this.logger.warn(s"[sql] sql不合法，在线调试功能只支持查询操作：json=$json")
        return msg.buildError(s"sql不合法，在线调试功能只支持查询操作", ErrorCode.ERROR)
      }

      if (this.baseFlink == null) {
        this.logger.warn(s"[sql] 系统正在初始化，请稍后再试：json=$json")
        return "系统正在初始化，请稍后再试"
      }

      ""
    } catch {
      case e: Exception => {
        this.logger.error(s"[sql] 执行用户sql失败：json=$json", e)
        msg.buildError("执行用户sql失败，异常堆栈：" + ExceptionBus.stackTrace(e), ErrorCode.ERROR)
      }
    }
  }
}
