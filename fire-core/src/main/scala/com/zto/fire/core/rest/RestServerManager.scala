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

import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.enu.ErrorCode
import com.zto.fire.common.util._
import com.zto.fire.predef._
import spark._

import java.net.ServerSocket
import scala.collection.mutable._

/**
 * Fire框架的rest服务管理器
 *
 * @author ChengLong 2019-3-16 09:56:56
 */
private[fire] class RestServerManager extends Logging {
  private[this] var port: JInt = null
  private[this] var restPrefix: String = _
  private[this] var socket: ServerSocket = _
  private[this] lazy val restList = ListBuffer[RestCase]()
  private[this] lazy val mainClassName: String = FireFrameworkConf.driverClassName

  /**
   * 注册新的rest接口
   *
   * @param rest
   * rest的封装信息
   * @return
   */
  private[fire] def addRest(rest: RestCase): this.type = {
    this.restList += rest
    this
  }

  /**
   * 获取Fire RestServer占用的端口号
   */
  def restPort: Int = this.port

  /**
   * 为rest服务指定监听端口
   */
  private[fire] def startRestPort(port: Int = 0): this.type = this.synchronized {
    if (this.port == null && !RestServerManager.isStarted) {
      Spark.threadPool(FireFrameworkConf.restfulMaxThread, 1, -1)
      // 端口占用失败默认重试3次
      if (port == 0) {
        retry(FireFrameworkConf.restfulPortRetryNum, FireFrameworkConf.restfulPortRetryDuration) {
          val randomPort = OSUtils.getRundomPort
          Spark.port(randomPort)
          this.port = randomPort
        }
      } else {
        Spark.port(port)
        this.port = port
      }
      // 获取到未被占用的端口后，rest server不会立即绑定，为了避免被其他应用占用
      // 此处使用ServerSocket占用该端口，等真正启动rest server前再关闭该ServerSocket以便释放端口
      this.socket = new ServerSocket(this.port)
      // 接口地址：hostname还是以ip地址
      val address = if (FireFrameworkConf.restUrlHostname) OSUtils.getHostName else OSUtils.getIp
      this.restPrefix = s"http://$address:${this.port}"
      PropUtils.setProperty(FireFrameworkConf.FIRE_REST_URL, s"$restPrefix")
    }
    this
  }

  /**
   * 注册并以子线程方式开启rest服务
   */
  private[fire] def startRestServer: Unit = this.synchronized {
    if (!FireFrameworkConf.restEnable || RestServerManager.isStarted) return
    RestServerManager.isStarted = true
    if (this.port == null) this.startRestPort()
    // 批量注册接口地址
    ThreadUtils.run {
      // 释放Socket占用的端口给RestServer使用，避免被其他服务所占用
      if (socket != null && !socket.isClosed) socket.close()
      restList.filter(_ != null).foreach(rest => {
        if (FireFrameworkConf.fireRestUrlShow) logger.info(s"---------> start rest: ${FirePS1Conf.wrap(restPrefix + rest.path, FirePS1Conf.BLUE, FirePS1Conf.UNDER_LINE)} successfully. <---------")
        rest.method match {
          case "get" | "GET" => Spark.get(rest.path, new Route {
            override def handle(request: Request, response: Response): AnyRef = {
              rest.fun(request, response)
            }
          })
          case "post" | "POST" => Spark.post(rest.path, new Route {
            override def handle(request: Request, response: Response): AnyRef = {
              rest.fun(request, response)
            }
          })
          case "put" | "PUT" => Spark.put(rest.path, new Route {
            override def handle(request: Request, response: Response): AnyRef = {
              rest.fun(request, response)
            }
          })
          case "delete" | "DELETE" => Spark.delete(rest.path, new Route {
            override def handle(request: Request, response: Response): AnyRef = {
              rest.fun(request, response)
            }
          })
        }
      })

      // 注册过滤器，用于进行权限校验
      Spark.before(new Filter {
        override def handle(request: Request, response: Response): Unit = {
          if (FireFrameworkConf.restFilter) {
            val msg = checkAuth(request)
            if (msg != null && msg.getCode != null && ErrorCode.UNAUTHORIZED == msg.getCode) {
              Spark.halt(401, msg.toString)
            }
          }
        }
      })
    }
  }

  /**
   * 通过header进行用户权限校验
   */
  private[fire] def checkAuth(request: Request): ResultMsg = {
    val auth = request.headers("Authorization")
    try {
      if (!EncryptUtils.checkAuth(auth, this.mainClassName)) {
        this.logger.warn(s"非法请求：用户身份校验失败！ip=${request.ip()} auth=$auth")
        ResultMsg.buildError(s"非法请求：用户身份校验失败！ip=${request.ip()}", ErrorCode.UNAUTHORIZED)
      }
    } catch {
      case e: Exception => {
        this.logger.error(s"非法请求：请检查请求参数！ip=${request.ip()} auth=$auth", e)
        ResultMsg.buildError(s"非法请求：请检查请求参数！ip=${request.ip()}", ErrorCode.UNAUTHORIZED)
      }
    }
    null
  }
}

private[fire] object RestServerManager {
  private[RestServerManager] var isStarted = false

  /**
   * 用于判断fire rest是否启动
   */
  def serverStarted: Boolean = this.isStarted
}
