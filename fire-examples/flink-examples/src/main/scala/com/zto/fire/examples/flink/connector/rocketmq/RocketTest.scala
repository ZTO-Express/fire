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

package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * Flink流式计算任务消费rocketmq
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-5-13 14:26:24
 */
object RocketTest extends BaseFlinkStreaming {

  override def process: Unit = {
    this.fire.createRocketMqPullStreamWithTag().print()
    // this.fire.createRocketMqPullStreamWithKey()
    // this.fire.createRocketMqPullStream()

    // 从另一个rocketmq中消费数据
    this.fire.createRocketMqPullStream(keyNum = 2).print()
    this.fire.start
  }



  def main(args: Array[String]): Unit = {
    this.init()
  }
}
