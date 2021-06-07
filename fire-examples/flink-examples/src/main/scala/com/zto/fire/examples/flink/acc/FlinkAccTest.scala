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

package com.zto.fire.examples.flink.acc

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.ext.function.FireMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * fire-flink计数器与自定义累加器的使用
 *
 * @author ChengLong 2020年1月11日 14:08:56
 * @since 0.4.1
 */
object FlinkAccTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.fire.createCollectionStream(1 to 100)
    // 使用内置的计数器
    this.testFlinkCounter(dstream)
  }

  /**
   * Fire中内置计数器的使用
   */
  def testFlinkCounter(dstream: DataStream[Int]): Unit = {
    // FireMapFunction功能较RichMapFunction等更为强大，推荐使用
    // 创建FireMapFunction类型的内部类，支持Map、MapPartition、FlatMap等操作
    // 在不同的map函数中进行累加全局有效
    dstream.map(new FireMapFunction[Int, Int]() {
      override def map(value: Int): Int = {
        // 多值计数器根据累加器的值类型区分不同的计数器，比如传参为Double类型，则累加至DoubleCounter中
        this.addCounter("LongCount", value.longValue())
        this.addCounter("IntCount", value)
        this.addCounter("IntCount2", value * 2)
        this.addCounter("DoubleCount", value.doubleValue())
        Thread.sleep(5000)
        value
      }
    })

    val result = this.fire.start

    // 获取计数器中的值
    val longCount = result.getAccumulatorResult[Long]("LongCount")
    println("累加值Long：" + longCount)
    val doubleCount = result.getAccumulatorResult[Double]("DoubleCount")
    println("累加值Double：" + doubleCount)
    val intCount = result.getAccumulatorResult[Integer]("IntCount")
    println("累加值IntCount：" + intCount)
    val intCount2 = result.getAccumulatorResult[Integer]("IntCount2")
    println("累加值IntCount2：" + intCount2)
    Thread.currentThread().join()

    this.stop
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
