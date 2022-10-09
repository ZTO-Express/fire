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

package com.zto.fire.examples.spark.module

import com.zto.fire._
import com.zto.fire.core.anno.connector._
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.anno.Streaming

@Hive("test")
@Streaming(interval = 10)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object ExceptionTest extends SparkCore {

  override def process: Unit = {
    // this.testSqlException
    this.testSqlException
  }

  /**
   * 测试SQL异常捕获
   */
  def testSqlException: Unit = {
    sql(
      """
        |use dim;
        |select ,'sh' as city from dw.mdb_md_dbs where ds='20211001' limit 100;
        |""".stripMargin).print()
  }

  /**
   * 测试API的异常捕获
   */
  def testApiException: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.map(t => {
      val a = 1 / 0
      t
    }).print()
  }
}