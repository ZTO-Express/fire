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

package com.zto.fire.examples.spark.jdbc

import com.zto.fire._
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.spark.BaseSparkStreaming

object JdbcStreamingTest extends BaseSparkStreaming {
  val tableName = "spark_test"

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()

    dstream.repartition(5).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val sql = s"select id from $tableName limit 1"
        JdbcConnector.executeQueryCall(sql, callback = _ => 1)
      })
    })

    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
