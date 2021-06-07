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

package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.spark.BaseSparkStreaming

/**
 * 消费rocketmq中的数据
 */
object RocketTest extends BaseSparkStreaming {
  override def process: Unit = {
    //读取RocketMQ消息流
    val dStream = this.fire.createRocketMqPullStream()
    dStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val source = rdd.map(msgExt =>  new String(msgExt.getBody).replace("messageBody", ""))
        import fire.implicits._
        this.fire.read.json(source.toDS()).createOrReplaceTempView("tmp_scanrecord")
        this.fire.sql(
          """
            |select *
            |from tmp_scanrecord
            |""".stripMargin).show(10,false)
      }
    })

    dStream.rocketCommitOffsets
    this.fire.start()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
