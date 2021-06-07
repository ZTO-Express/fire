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

import org.apache.log4j.{Level, Logger}
import org.junit.Test

/**
 * shutdown hook管理器单元测试
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-20 14:45
 */
class ShutdownHookManagerTest {
  Logger.getLogger(classOf[ShutdownHookManagerTest]).setLevel(Level.toLevel("INFO"))

  @Test
  def testRegister: Unit = {
    ShutdownHookManager.addShutdownHook(1) {
      () => println("1. 执行逻辑")
    }
    ShutdownHookManager.addShutdownHook(3) {
      () => println("3. 执行逻辑")
    }
    ShutdownHookManager.addShutdownHook(2) {
      () => println("2. 执行逻辑")
    }
    ShutdownHookManager.addShutdownHook(5) {
      () => println("5. 执行逻辑")
    }
    println("=========main method==========")
  }
}
