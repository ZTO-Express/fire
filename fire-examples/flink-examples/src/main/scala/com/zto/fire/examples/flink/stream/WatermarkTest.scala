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
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils}
import com.zto.fire.core.anno.{Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.ext.watermark.FirePeriodicWatermarks
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat

/**
 * 水位线的使用要求：
 * 1. 开启EventTime：flink.stream.time.characteristic = EventTime
 * 2. 不同的task中有多个水位线实例，本地测试为了尽快看到效果，要降低并行度
 * 3. 多个task中的水位线会取最早的
 * 4. 水位线触发条件：1）多个task中时间最早的水位线时间 >= window窗口end时间  2）窗口中有数据
 * 5. 水位线是为了解决乱序和延迟数据的问题
 * 6. 乱序数据超过水位线的三种处理方式：1. 丢弃（默认） 2. allowedLateness，相当于进一步宽容的时间 3. sideOutputLateData：将延迟数据收集起来，统一处理
 *
 * @author ChengLong 2020-4-13 15:58:38
 */
@Config(
  """
    |flink.stream.time.characteristic    =       EventTime
    |flink.default.parallelism           =       2
    |""")
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object WatermarkTest extends BaseFlinkStreaming {

  override def process: Unit = {
    // source端接入消息并解析
    val dstream = this.fire.createKafkaDirectStream().filter(str => StringUtils.isNotBlank(str) && str.contains("}")).map(str => {
      val student = JSONUtils.parseObject[Student](str)
      (student, DateFormatUtils.formatDateTime(student.getCreateTime).getTime)
    })

    // 分配并计算水位线，默认允许最大的乱序时间为10s，若需指定，则通过构造方法传参new FirePeriodicWatermarks(100)
    val watermarkDS = dstream.assignTimestampsAndWatermarks(new FirePeriodicWatermarks[(Student, Long)]() {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      /**
       * 抽取eventtime字段
       */
      override def extractTimestamp(element: (Student, Long), previousElementTimestamp: Long): Long = {
        println("---> 抽取eventtime：" + element._2 + " 最新水位线值：" + this.watermark.getTimestamp)
        element._2
      }

    }).setParallelism(1) // 并行度调整为1的好处是能尽快观察到水位线的效果，否则要等多个task满足条件，不易观察结果

    val windowDStream = watermarkDS
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      // 最大允许延迟的数据3s，算上水位线允许最大的乱序时间10s，一共允许最大的延迟时间为13s
      .allowedLateness(Time.seconds(3))
      // 收集延期的数据
      .sideOutputLateData(this.outputTag.asInstanceOf[OutputTag[(Student, Long)]])
      .apply(new WindowFunctionTest)

    windowDStream.print().setParallelism(1)
    // 获取由于延迟太久而被丢弃的数据
    windowDStream.getSideOutput[(Student, Long)](this.outputTag.asInstanceOf[OutputTag[(Student, Long)]]).map(t => ("丢弃", t)).print()

    this.fire.start
  }

  /**
   * 泛型说明：
   * 1. IN: The type of the input value.
   * 2. OUT: The type of the output value.
   * 3. KEY: The type of the key.
   */
  class WindowFunctionTest extends WindowFunction[(Student, Long), (Student, Long), Student, TimeWindow] {
    override def apply(key: Student, window: TimeWindow, input: Iterable[(Student, Long)], out: Collector[(Student, Long)]): Unit = {
      println("-->" + JSONUtils.toJSONString(key))
      val sortedList = input.toList.sortBy(_._2)
      sortedList.foreach(t => {
        println("---> " + JSONUtils.toJSONString(t._1))
        out.collect(t)
      })
    }
  }
}
