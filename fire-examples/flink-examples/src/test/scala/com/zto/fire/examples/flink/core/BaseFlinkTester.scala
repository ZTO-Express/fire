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

package com.zto.fire.examples.flink.core

import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire._
import org.junit.{After, Before}

/**
 * Flink 单元测试父接口，用于初始化fire与flink上下文
 *
 * @author ChengLong
 * @date 2022-05-17 09:55:30
 * @since 2.2.2
 */
trait BaseFlinkTester extends BaseFlinkStreaming {

  /**
   * 初始化fire框架与flink相关的运行时上下文
   */
  @Before
  def before: Unit = {
    this.init()
  }

  @After
  override def after: Unit = {
    // this.fire.start("JdbcTest")
  }
}
