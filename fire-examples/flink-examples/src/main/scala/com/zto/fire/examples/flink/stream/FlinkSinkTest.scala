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

package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * 自定义sink的实现
 */
object FlinkSinkTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createDirectStream().map(json => JSONUtils.parseObject[Student](json))
    dstream.map(t => t.getName).addSink(new MySink).setParallelism(1)

    this.fire.start
  }
}

class MySink extends RichSinkFunction[String] {

  /**
   * open方法中可以创建数据库连接等初始化操作
   * 注：若setParallelism(10)则会执行10次open方法
   */
  override def open(parameters: Configuration): Unit = {
    println("=========执行open方法========")
  }

  /**
   * close方法用于释放资源，如数据库连接等
   */
  override def close(): Unit = {
    println("=========执行close方法========")
  }

  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    println("---> " + value)
  }
}
