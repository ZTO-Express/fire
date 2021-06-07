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

import com.zto.fire.common.util.ExceptionBus.stackTrace
import com.zto.fire.predef._
import org.junit.Assert._
import org.junit.Test

/**
 * 用于ExceptionBus的单元测试
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-16 14:42
 */
class ExceptionBusTest {

  /**
   * 用于测试queue大小限制与exception的存入和获取
   */
  @Test
  def testTry: Unit = {
    (1 to 10020).foreach(i => {
      tryWithLog {
        val a = 1 / 0
      } (isThrow = false)
    })

    val t = ExceptionBus.getAndClear
    assertEquals(t._1.size, 1000)
    t._1.foreach(t => stackTrace(t._2))

    // 上一次获取后queue中的记录数为0
    assertEquals(ExceptionBus.queueSize.get(), 0)
    assertEquals(ExceptionBus.exceptionCount.get(), 10020)
  }

}
