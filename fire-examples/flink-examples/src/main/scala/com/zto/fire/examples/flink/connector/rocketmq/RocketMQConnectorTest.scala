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
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
object RocketMQConnectorTest extends BaseFlinkStreaming {

  override def process: Unit = {
    this.fire.sql("""
                    |CREATE table source (
                    |  id bigint,
                    |  name string,
                    |  age int,
                    |  length double,
                    |  data DECIMAL(10, 5)
                    |) WITH
                    |   (
                    |   'connector' = 'fire-rocketmq',
                    |   'format' = 'json',
                    |   'rocket.brokers.name' = 'ZmsClusterX',
                    |   'rocket.topics'       = 'fire',
                    |   'rocket.group.id'     = 'fire',
                    |   'rocket.consumer.tag' = '*'
                    |   )
                    |""".stripMargin)

    this.fire.sql(
      """
        |select * from source
        |""".stripMargin).print()
  }
}
