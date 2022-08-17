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
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.flink.FlinkStreaming
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._

/**
 * flink重分区
 *
 * @author ChengLong 2020-4-10 09:50:26
 */
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object FlinkPartitioner extends FlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createCollectionStream(1 to 10)
    // 将当前所有输出值都输出到下游算子的第一个实例中，会导致严重的性能问题，谨慎使用
    dstream.global.print()
    // 将当前输出中的每一条记录随机输出到下游的每一个实例中，可显著解决数据倾斜问题
    dstream.shuffle.print()
    // 将当前输出以循环的方式输出到下游算子的每一个实例中，可显著解决数据倾斜问题，比shuffle方式分配的更均匀
    dstream.rebalance.print()
    // 基于上下游Operator的并行度，将记录以循环的方式输出到下游Operator的每个实例。举例: 上游并行度是2，下游是4，
    // 则上游一个并行度以循环的方式将记录输出到下游的两个并行度上;上游另一个并行度以循环的方式将记录输出到下游另两个并行度上。
    // 若上游并行度是4，下游并行度是2，则上游两个并行度将记录输出到下游一个并行度上；上游另两个并行度将记录输出到下游另一个并行度上
    // 相当于小范围的rebalance操作
    dstream.rescale.print()
    // 将上游数据全部输出到下游每一个算子的实例中，适合于大数据集Join小数据集的场景
    dstream.broadcast.print()
    // 将记录输出到下游本地的operator实例，ForwardPartitioner分区器要求上下游算子并行度一样，上下游Operator同属一个SubTasks
    dstream.forward.print()
    // 将记录按Key的Hash值输出到下游Operator实例
    // dstream.map(t => (t, t)).keyBy(KeySelector[Int, Int]())
    // 自定义分区，需继承Partitioner并实现自己的partition分区算法
    dstream.map(t => (t, t)).partitionCustom(new HashPartitioner, 0).print()
  }

  /**
   * Flink自定义分区
   */
  class HashPartitioner extends Partitioner[Int] {
    override def partition(key: Int, numPartitions: Int): Int = {
      if (key % 2 == 0) {
        0
      } else {
        1
      }
    }
  }
}
