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

package com.zto.fire.common.util

import com.zto.fire.predef._

import java.util.PriorityQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Fire框架统一的shutdown hook管理器，所有注册了的hook将会在jvm退出前根据优先级依次调用
 *
 * @author ChengLong
 * @create 2020-11-20 14:06
 * @since 1.1.2
 */
private[fire] class ShutdownHookManager extends Logging  {
  // 具有优先级的队列，存放各处注册的hook信息，在jvm退出前根据优先级依次调用
  private[this] val hooks = new PriorityQueue[HookEntry]()
  private[this] val shuttingDown = new AtomicBoolean(false)

  /**
   * 执行所有的hook
   */
  def runAll: Unit = {
    if (this.shuttingDown.compareAndSet(false, true)) {
      var nextHook: HookEntry = null
      while ( {
        nextHook = hooks.synchronized {
          hooks.poll()
        };
        nextHook != null
      }) {
        // 调用每一个hook的run方法
        tryWithLog(nextHook.run())(this.logger, tryLog = "Fire shutdown hook executed.", catchLog = "执行hook过程中发生例外.")
      }
    }
  }

  /**
   * install所有的hook
   */
  def install: Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      // 调用hooks中的所有hook的run方法，每个run都会被try/cache包围
      override def run(): Unit = runAll
    })
  }

  /**
   * 添加指定优先级的hook
   */
  def add(priority: Int, hook: () => Unit): Unit = {
    this.hooks.synchronized {
      if (this.shuttingDown.get()) throw new IllegalStateException("Shutdown hooks 在关闭过程中无法注册新的hook")
      this.hooks.add(new HookEntry(priority, hook))
    }
  }

  /**
   * 移除指定的hook
   */
  def remove(ref: AnyRef): Unit = {
    this.hooks.synchronized {
      this.hooks.remove(ref)
    }
  }
}

/**
 * hook项，包含优先级与具体的hook逻辑
 *
 * @param priority
 * hook优先级，优先级高的会先被调用
 * @param hook
 * hook具体的执行逻辑，比如用于关闭数据库连接等
 */
private[fire] class HookEntry(private val priority: Int, hook: () => Unit) extends Comparable[HookEntry] {

  /**
   * hook执行顺序的优先级比较
   */
  override def compareTo(o: HookEntry): Int = o.priority - this.priority

  /**
   * run方法中调用hook函数
   */
  def run(): Unit = hook()
}

/**
 * Fire框架统一的shutdown hook管理器
 * 调用者可以基于提供的api进行hook的注册
 */
object ShutdownHookManager {
  // 优先级定义
  lazy val DEFAULT_PRIORITY = 10
  private[fire] lazy val HEIGHT_PRIORITY = 100
  val LOW_PRIORITY = 5
  private[this] lazy val hookManager = new ShutdownHookManager()

  this.hookManager.install

  def addShutdownHook(priority: Int = DEFAULT_PRIORITY)(hook: () => Unit): Unit = {
    hookManager.add(priority, hook)
  }

  def removeShutdownHook(ref: AnyRef): Unit = this.hookManager.remove(ref)
}