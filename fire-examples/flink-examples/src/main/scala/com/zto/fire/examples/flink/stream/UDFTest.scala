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
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.ScalarFunction

/**
 * 自定义udf测试
 *
 * @author ChengLong 2020年1月13日 10:36:39
 * @since 0.4.1
 */
object UDFTest extends BaseFlinkStreaming {
  override def process: Unit = {
    this.flink.setParallelism(10)
    val dataset = this.flink.createCollectionStream(Student.newStudentList()).map(t => t).setParallelism(5)
    this.tableEnv.registerDataStream("test", dataset)
    // 注册udf
    this.tableEnv.createTemporarySystemFunction("appendFire", classOf[Udf])
    // 在sql中使用自定义的udf
    this.flink.sql("select fireUdf(name), fireUdf(age) from test").print()
    dataset.print("dataset")

    this.flink.execute()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }

}


class Udf extends ScalarFunction {
  /**
   * 为指定字段的值追加fire字符串
   *
   * @param field
   * 字段名称
   * @return
   * 追加fire字符串后的字符串
   */
  def eval(field: String): String = field + "->fire"

  /**
   * 支持函数的重载，会自动判断输入字段的类型调用相应的函数
   */
  def eval(field: JInt): String = field + "-> Int fire"
}