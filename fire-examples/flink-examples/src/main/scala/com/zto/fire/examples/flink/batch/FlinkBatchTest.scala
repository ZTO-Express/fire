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

package com.zto.fire.examples.flink.batch

import com.zto.fire._
import com.zto.fire.flink.FlinkBatch
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/**
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
object FlinkBatchTest extends FlinkBatch {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    this.testAccumulator
  }

  def testAccumulator: Unit = {
    val result = this.fire.createCollectionDataSet(1 to 10).map(new RichMapFunction[Int, Int] {
      val counter = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        this.getRuntimeContext.addAccumulator("myCounter", this.counter)
      }

      override def map(value: Int): Int = {
        this.counter.add(value)
        value
      }
    })
    result.writeAsText("J:\\test\\flink.result", FileSystem.WriteMode.OVERWRITE)

    val result2 = this.fire.createCollectionDataSet(1 to 10).map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        this.getRuntimeContext.getIntCounter("myCounter").add(value)
        value
      }
    })
    result2.writeAsText("J:\\test\\flink.result", FileSystem.WriteMode.OVERWRITE)
    val count = this.fire.execute("counter").getAccumulatorResult[Int]("myCounter")
    println("累加器结果：" + count)
  }
}
