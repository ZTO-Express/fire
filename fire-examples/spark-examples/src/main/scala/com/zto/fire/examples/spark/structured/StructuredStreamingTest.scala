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

package com.zto.fire.examples.spark.structured

import com.zto.fire._
import com.zto.fire.spark.BaseStructuredStreaming

/**
 * 使用fire进行structured streaming开发的demo
 *
 * @author ChengLong 2019年12月23日 22:16:59
 */
object StructuredStreamingTest extends BaseStructuredStreaming {

  /**
   * structured streaming处理逻辑
   */
  override def process: Unit = {
    // 接入kafka消息，并将消息解析为DataFrame，同时注册临时表，表名默认为kafka，也可传参手动指定表名
    val kafkaDataset = this.fire.loadKafkaParseJson()
    // 进行sql查询，支持嵌套的json，并且支持大小写的json
    this.fire.sql("select table, after.bill_code, after.scan_site from kafka").print()
    // 使用api的方式进行查询操作
    kafkaDataset.select("after.PDA_CODE", "after.bill_code").print(numRows = 1, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}