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

package com.zto.fire.examples.spark.core

import com.zto.fire._
import com.zto.fire.spark.BaseSpark
import org.junit.{After, Before}

/**
 * Spark 单元测试父接口，用于初始化fire与spark上下文
 *
 * @author ChengLong
 * @date 2022-05-11 10:47:57
 * @since 2.2.2
 */
trait SparkTester extends BaseSpark {

  /**
   * 初始化fire框架与spark相关的运行时上下文
   */
  @Before
  def before: Unit = {
    this.init()
  }

  /**
   * 注销fire框架与spark的上下文信息
   */
  @After
  override def after: Unit = {
    if (noEmpty(this.sc, this.fire) && this.sc.isStopped) this.stop
  }
}
