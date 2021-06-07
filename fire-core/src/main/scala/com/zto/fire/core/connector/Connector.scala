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

package com.zto.fire.core.connector

import com.zto.fire.common.conf.FireFrameworkConf

import java.util.concurrent.ConcurrentHashMap
import com.zto.fire.predef._
import com.zto.fire.common.util.ShutdownHookManager
import org.slf4j.{Logger, LoggerFactory}

/**
 * connector父接口，约定了open与close方法，子类需要根据具体
 * 情况覆盖这两个方法。这两个方法不需要子类主动调用，会被自动调用
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-11-27 10:32
 */
private[fire] trait Connector extends Serializable {
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  this.hook()

  /**
   * 用于注册释放资源
   */
  private[this] def hook(): Unit = {
    if (FireFrameworkConf.connectorShutdownHookEnable) {
      ShutdownHookManager.addShutdownHook() { () => {
        this.close()
        logger.info("release connector successfully.")
      }
      }
    }
  }

  /**
   * connector资源初始化
   */
  protected[fire] def open(): Unit = {
    this.logger.debug("init connector.")
  }

  /**
   * connector资源释放
   */
  protected def close(): Unit = {
    this.logger.debug("close connector.")
  }
}

/**
 * 支持多集群的connector
 *
 * @param keyNum
 * 对应的connector实例标识，不同的keyNum对应不同的集群连接实例
 */
private[fire] abstract class FireConnector(keyNum: Int = 1) extends Connector

/**
 * 用于根据指定的keyNum创建不同的connector实例
 */
private[fire] abstract class ConnectorFactory[T <: Connector] extends Serializable {
  @transient
  private[fire] lazy val instanceMap = new ConcurrentHashMap[Int, T]()
  @transient
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 约定创建connector子类实例的方法
   */
  protected def create(conf: Any = null, keyNum: Int = 1): T

  /**
   * 根据指定的keyNum返回单例的HBaseConnector实例
   */
  def getInstance(keyNum: Int = 1): T = this.instanceMap.get(keyNum)

  /**
   * 创建指定集群标识的connector对象实例
   */
  def apply(conf: Any = null, keyNum: Int = 1): T = {
    this.instanceMap.mergeGet(keyNum) {
      val instance: T = this.create(conf, keyNum)
      instance.open()
      instance
    }
  }
}