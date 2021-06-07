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

package com.zto.fire.flink.ext.stream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * 用于对Flink KeyedStream的API库扩展
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-15 10:20
 */
class KeyedStreamExt[T, K](keyedStream: KeyedStream[T, K]) {

  /**
   * 创建滑动窗口
   *
   * @param size
   *                           窗口的大小
   * @param slide
   *                           窗口滑动间隔
   * @param offset
   *                           时区
   * @param timeCharacteristic 时间类别
   */
  def slidingTimeWindow[W <: Window](size: Time, slide: Time, offset: Time = Time.milliseconds(0), timeCharacteristic: TimeCharacteristic = TimeCharacteristic.ProcessingTime): WindowedStream[T, K, W] = {
    if (timeCharacteristic == TimeCharacteristic.EventTime) {
      keyedStream.window(SlidingEventTimeWindows.of(size, slide, offset).asInstanceOf[WindowAssigner[T, W]])
    } else {
      keyedStream.window(SlidingProcessingTimeWindows.of(size, slide, offset).asInstanceOf[WindowAssigner[T, W]])
    }
  }

  /**
   * 创建滚动窗口窗口
   *
   * @param size
   *                           窗口的大小
   * @param offset
   *                           时区
   * @param timeCharacteristic 时间类别
   */
  def tumblingTimeWindow[W <: Window](size: Time, offset: Time = Time.milliseconds(0), timeCharacteristic: TimeCharacteristic = TimeCharacteristic.ProcessingTime): WindowedStream[T, K, W] = {
    if (timeCharacteristic == TimeCharacteristic.EventTime) {
      keyedStream.window(TumblingEventTimeWindows.of(size, offset).asInstanceOf[WindowAssigner[T, W]])
    } else {
      keyedStream.window(TumblingProcessingTimeWindows.of(size, offset).asInstanceOf[WindowAssigner[T, W]])
    }
  }

  /**
   * 创建session会话窗口
   *
   * @param size
   *                           超时时间
   * @param timeCharacteristic 时间类别
   */
  def sessionTimeWindow[W <: Window](size: Time, timeCharacteristic: TimeCharacteristic = TimeCharacteristic.ProcessingTime): WindowedStream[T, K, W] = {
    if (timeCharacteristic == TimeCharacteristic.EventTime) {
      keyedStream.window(EventTimeSessionWindows.withGap(size).asInstanceOf[WindowAssigner[T, W]])
    } else {
      keyedStream.window(ProcessingTimeSessionWindows.withGap(size).asInstanceOf[WindowAssigner[T, W]])
    }
  }
}
