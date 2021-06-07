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
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.examples.spark.sql.LoadTestSQL
import com.zto.fire.spark.BaseSparkStreaming

/**
  * kafka json解析
  * @author ChengLong 2019-6-26 16:52:58
  */
object LoadTest extends BaseSparkStreaming {

  /**
   * 缓存维表
   */
  def loadNewConfigTable: Unit ={
    spark.sql(LoadTestSQL.cacheDim).cache().createOrReplaceTempView("dim_c2c_cost")
  }

  /**
   * 重复压测
   */
  @Scheduled(fixedInterval = 1000 * 60 * 1, concurrent = false, initialDelay = 1000 * 30)
  def reload(): Unit = {
    this.fire.sql(LoadTestSQL.loadSQL).show(10, false)
  }

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    this.loadNewConfigTable

    val dstream = this.fire.createKafkaDirectStream()
    dstream.foreachRDD(rdd => {
      if (rdd.isNotEmpty) {
        // 一、将json解析并注册为临时表，默认不cache临时表
        rdd.kafkaJson2Table("test", cacheTable = true)
        // toLowerDF表示将大写的字段转为小写
        this.fire.sql("select after.* from test").toLowerDF.show(1, false)
        this.fire.sql(LoadTestSQL.jsonParseSQL)
      }
    })

    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
    this.stop
  }
}
