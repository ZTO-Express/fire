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
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.Kafka
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.ScalarFunction

/**
 * 自定义udf测试
 *
 * @author ChengLong 2020年1月13日 10:36:39
 * @since 0.4.1
 */
@Config(
  """
    |# 开启fire udf注册功能（默认为关闭）
    |flink.sql.udf.fireUdf.enable=true
    |# 指定udf jar包的本地路径
    |flink.sql.conf.pipeline.jars=file:///home/spark3/flink/udf.jar
    |# 指定udf函数名为appendFire，对应的udf实现类为com.zto.fire.examples.flink.stream.Udf
    |flink.sql.udf.conf.appendFire=com.zto.fire.examples.flink.stream.Udf
    |flink.sql.udf.conf.fire=com.zto.fire.examples.flink.stream.Udf
    |""")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object UDFTest extends FlinkStreaming {
  override def process: Unit = {
    val stream = this.fire.createKafkaDirectStream()
      .map(JSONUtils.parseObject[Student](_)).setParallelism(3)
    stream.createOrReplaceTempView("test")
    // 在sql中使用自定义的udf
    this.flink.sql("select appendFire(name), fire(age) from test").print()
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