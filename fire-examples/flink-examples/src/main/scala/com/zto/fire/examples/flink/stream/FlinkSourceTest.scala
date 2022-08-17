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
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.flink.FlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 自定义source
 * @author ChengLong 2020-4-7 14:30:08
 */
@Hive("test")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object FlinkSourceTest extends FlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.addSource(new MySource).setParallelism(2)
    // 注意Time的包不要导错，来自org.apache.flink.streaming.api.windowing.time.Time
    dstream.timeWindowAll(Time.seconds(2)).sum(0).setParallelism(1).print
  }
}

/**
 * 自定义source组件
 * 支持多并行度
 */
class MySource extends RichParallelSourceFunction[Long] {
  private var isRunning = false
  private var index = 1

  /**
   * open方法中可以创建数据库连接等初始化操作
   * 注：若setParallelism(10)则会执行10次open方法
   */
  override def open(parameters: Configuration): Unit = {
    this.isRunning = true
    println("=========执行open方法========")
  }

  /**
   * 持续不断的将消息发送给flink
   * @param ctx
   */
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (this.isRunning) {
      this.index += 1
      ctx.collect(this.index)
      Thread.sleep(1000)
    }
  }

  /**
   * 当任务被cancel时调用
   */
  override def cancel(): Unit = {
    this.isRunning = false
    println("=========执行cancel方法==========")
  }

  /**
   * close方法用于释放资源，如数据库连接等
   */
  override def close(): Unit = {
    println("=========执行close方法==========")
  }
}
