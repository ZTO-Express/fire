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

package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
 * 基于DataGenReceiver来随机生成测试数据集
 *
 * @author ChengLong 2022-03-07 15:35:55
 * @since 2.2.1
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(20)
object DataGenTest extends SparkStreaming {

  override def process: Unit = {
    // 方式一、在JavaBean中实现generate方法，在该方法中定义对象生成的规则
    val dstream = this.fire.createBeanGenStream[Student](10)
    dstream.print(1)

    // 方式二、通过实现generateFun函数来定义数据生成规则
    val dstream2 = this.fire.createDataGenStream(10, generateFun = Student.newStudentList())
    dstream2.print(1)
  }
}
