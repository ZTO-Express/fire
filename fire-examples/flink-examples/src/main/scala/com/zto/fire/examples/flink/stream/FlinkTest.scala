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
import com.zto.fire.common.util.{JSONUtils, PropUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.util.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object FlinkTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    /*val dstream = this.fire.createKafkaDirectStream().filter(str => JsonUtils.isJson(str)).map(json => {
      JsonUtils.parseObject[Student](json)
    }).setParallelism(2)

    dstream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select * from student")
    println("fire.rest.url========>" + PropUtils.getString("fire.rest.url", "not_found"))
    // toRetractStream支持状态更新、删除操作，比例sql中含有group by 等聚合操作，后进来的记录会导致已有的聚合结果不正确
    // 使用toRetractStream后会将之前的旧的聚合结果重新发送一次，并且tuple中的flag标记为false，然后再发送一条正确的结果
    // 类似于structured streaming中自动维护结果表，并进行update操作
    this.tableEnv.toRetractStream[Row](table).map(t => t._2).addSink(row => {
      println("fire.rest.url========>" + PropUtils.getString("fire.rest.url", "not_found"))
      println("是否为TaskManager========>" + FlinkUtils.isJobManager)
      println("运行模式========>" + FlinkUtils.runMode)
    })*/
    this.fire.createKafkaDirectStream().map(t => {
      this.logger.info(t)
    }).print()

    // 不指定job name，则默认当前类名
    // this.fire.start
    this.fire.start("Fire Test")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
