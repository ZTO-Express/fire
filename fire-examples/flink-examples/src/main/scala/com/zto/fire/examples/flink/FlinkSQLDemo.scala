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

package com.zto.fire.examples.flink

import com.zto.fire.core.anno.lifecycle.{Step1, Step2, Step3}
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * 基于Fire进行flink sql开发
 * 使用@Step注解的无参方法将按数值顺序依次被Fire框架调用：
 * <p><i>Step1. 定义源表表结构 </i>
 * <i>Step1. 执行耗时：105.00ms </i>
 *
 * <i>Step2. 定义目标表结构 </i>
 * <i>Step2. 执行耗时：5.00ms </i>
 *
 * <i>Step3. 执行insert语句 </i>
 * <i>Step3. 执行耗时：322.00ms </i>
 *
 * <i>Finished. 总计：3个 成功：3个 失败：0个, 执行耗时：433.00ms </i></p>
 *
 * @author ChengLong
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(interval = 10)
object FlinkSQLDemo extends FlinkStreaming {

  @Step1("定义源表表结构")
  def sourceTable: Unit = {
    sql(s"""
           | CREATE TABLE t_student (
           |   id BIGINT,
           |   name STRING,
           |   age INT,
           |   createTime TIMESTAMP(13),
           |   sex Boolean
           |) WITH (
           |   'connector' = 'datagen',
           |   'rows-per-second'='100', -- 5000/s
           |   'fields.id.min'='1', -- id字段，1到1000之间
           |   'fields.id.max'='1000',
           |   'fields.name.length'='5', -- name字段，长度为5
           |   'fields.age.min'='1', -- age字段，1到120岁
           |   'fields.age.max'='120'
           |)
           |""".stripMargin)
  }

  @Step2("定义目标表结构")
  def destTable: Unit = {
    sql(s"""
           |CREATE TABLE t_print_table WITH ('connector' = 'print')
           |LIKE t_student (EXCLUDING ALL)
           |""".stripMargin)
  }

  @Step3("执行insert语句")
  def insertStatement: Unit = {
    sql(
      s"""
         |insert into t_print_table
         |select
         | id, name, age, createTime, sex
         |from t_student
         |group by id, name, age, createTime, sex
         |""".stripMargin)
  }
}
