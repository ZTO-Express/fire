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
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.Kafka
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * window相当于将源源不断的流按一定的规则切分成有界流，然后为每个有界流分别计算
 * 当程序挂掉重启后，window中的数据不会丢失，会接着之前的window继续计算
 * 注：不建议使用windowAll，该api会将数据发送到同一个分区，造成严重的性能问题
 *
 * @author ChengLong 2020-4-18 14:34:58
 */
@Config(
  """
    |flink.fire.rest.filter.enable       =       false
    |flink.default.parallelism           =       8
    |flink.max.parallelism               =       8
    |""")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object WindowTest extends FlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().map(t => JSONUtils.parseObject[Student](t)).map(s => (s.getName, s.getAge))
    this.testTimeWindow(dstream)
  }

  /**
   * 如果是keyedStream，则窗口函数为countWindow
   */
  private def testCountWindow(dstream: DataStream[(String, Integer)]): Unit = {
    dstream.keyBy(_._1)
      // 第一个参数表示窗口大小，窗口的容量是2条记录，达到2条会满，作为一个单独的window实例
      // 第二个参数如果不指定，则表示为滚动窗口（没有重叠），如果指定则为滑动窗口（有重叠）
      // 以下表示每隔1条数据统计一次window数据，而这个window中包含2条记录
      .countWindow(2, 1)
      .sum(1).print()
  }

  /**
   * 如果是普通的Stream，则窗口函数为countWindowAll
   */
  def testCountWindowAll(dstream: DataStream[(String, Integer)]): Unit = {
    // 表示每2条计算一次，每次将计算好的两条记录结果打印
    dstream.countWindowAll(2).sum(1).print()
  }

  /**
   * 时间窗口
   */
  def testTimeWindow(dstream: DataStream[(String, Integer)]): Unit = {
    // 窗口的宽度为1s，每隔1s钟处理过去1s的数据，这1s的时间内窗口中的记录数可多可少
    dstream.timeWindowAll(Time.seconds(1)).sum(1).print()
    // 创建一个基于process时间（支持event时间）的滑动窗口，窗口大小为10秒，每隔5秒创建一个
    dstream.keyBy(_._1).slidingTimeWindow(Time.seconds(10), Time.seconds(5), timeCharacteristic = TimeCharacteristic.ProcessingTime).sum(1).printToErr()
    // 创建一个滚动窗口
    dstream.keyBy(_._1).tumblingTimeWindow(Time.seconds(10)).sum(1).print()
    // 创建一个session会话窗口，当5秒内没有消息进入，则单独划分一个窗口
    dstream.keyBy(_._1).sessionTimeWindow(Time.seconds(5)).sum(1).printToErr()
  }
}
