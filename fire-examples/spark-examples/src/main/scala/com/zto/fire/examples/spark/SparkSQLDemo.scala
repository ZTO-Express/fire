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

package com.zto.fire.examples.spark

import com.zto.fire.core.anno.lifecycle.{Step1, Step2}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkCore

/**
 * 基于Fire进行spark sql开发
 * 使用@Step注解的无参方法将按数值顺序依次被Fire框架调用：
 * <p><i>Step1. 定义数据集 </i>
 * <i>Step1. 执行耗时：482.00ms </i>
 *
 * <i>Step2. 统计记录数 </i>
 *  +--------+
 *  |count(1)|
 *  +--------+
 *  |       9|
 *  +--------+
 * <i>Step2. 执行耗时：1.43s </i>
 *
 * <i>Finished. 总计：2个 成功：2个 失败：0个, 执行耗时：1.92s </i></p>
 *
 * @author ChengLong
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
object SparkSQLDemo extends SparkCore {

  @Step1("定义数据集")
  def createDF: Unit = {
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    df.createOrReplaceTempView("student")
  }

  @Step2("统计记录数")
  def count: Unit = {
    sql(
      """
        |select count(1) from student;
        |
        |select count(1) from student;
        |""".stripMargin).show()
  }
}
