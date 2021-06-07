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

package com.zto.fire.core.rest

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.enu.{Datasource, ErrorCode}
import com.zto.fire.common.util.{DatasourceDesc, JSONUtils}
import com.zto.fire.core.BaseFire
import com.zto.fire.predef.{JHashMap, JHashSet, _}
import org.slf4j.{Logger, LoggerFactory}
import spark.{Request, Response}

/**
 * 系统预定义的restful服务抽象
 *
 * @author ChengLong 2020年4月2日 13:58:08
 */
protected[fire] abstract class SystemRestful(engine: BaseFire) {
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  // 用于记录当前任务所访问的数据源
  private lazy val datasourceMap = new JConcurrentHashMap[Datasource, JHashSet[DatasourceDesc]]()
  this.register

  /**
   * 注册接口
   */
  protected def register: Unit

  /**
   * 获取当前任务所使用到的数据源信息
   *
   * @return
   * 数据源列表
   */
  @Rest("/system/datasource")
  protected def datasource(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg
    try {
      val dataSource = JSONUtils.toJSONString(this.datasourceMap)
      this.logger.info(s"[DataSource] 获取数据源列表成功：counter=$dataSource")
      msg.buildSuccess(dataSource, "获取数据源列表成功")
    } catch {
      case e: Exception => {
        this.logger.error(s"[log] 获取数据源列表失败", e)
        msg.buildError("获取数据源列表失败", ErrorCode.ERROR)
      }
    }
  }

  @Rest("/system/collectDatasource")
  def collectDatasource(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg
    try {
      val json = request.body()
      val datasource = JSONUtils.parseObject[JHashMap[Datasource, JHashSet[DatasourceDesc]]](json)
      if (datasource.nonEmpty) this.datasourceMap.putAll(datasource)
      msg.buildSuccess(datasource, "添加数据源列表成功")
    }catch {
      case e: Exception => {
        this.logger.error(s"[log] 添加数据源列表失败", e)
        msg.buildError("添加数据源列表失败", ErrorCode.ERROR)
      }
    }
  }
}
