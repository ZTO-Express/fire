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
import com.zto.fire.flink.BaseFlinkBatch
import com.zto.fire.flink.ext.function.FireMapFunction
import org.apache.flink.api.scala._

/**
 * flink广播变量的使用
 *
 * @author ChengLong 2020年2月18日 13:53:06
 */
object FlinkBrocastTest extends BaseFlinkBatch {

  override def process: Unit = {
    val ds = this.fire.createCollectionDataSet(Seq(1, 2, 3, 4, 5))
    // flink中可以广播的数据必须是Dataset
    val brocastDS = this.fire.createCollectionDataSet(Seq("a", "b", "c", "d", "e"))

    ds.map(new FireMapFunction[Int, String] {
      // 获取广播变量中的值给当前成员变量（若不想在open方法中获取值，请使用lazy关键字）
      lazy val broadcastSet: Seq[String] = this.getBroadcastVariable[String]("brocastDS")

      override def map(value: Int): String = {
        this.broadcastSet(value - 1)
      }

      // 每次使用必须通过withBroadcastSet进行广播
    }).withBroadcastSet(brocastDS, "brocastDS").print()
  }
}
