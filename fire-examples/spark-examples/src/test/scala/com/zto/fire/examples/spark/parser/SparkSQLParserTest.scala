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

package com.zto.fire.examples.spark.parser

import com.zto.fire.common.anno.{Config, TestStep}
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.core.SparkTester
import com.zto.fire.println
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.sql.SparkSqlParser
import org.junit.Test

/**
 * 用于测试Spark SQL解析器
 *
 * @author ChengLong
 * @date 2022年09月06日 13:58:59
 * @since 2.3.2
 */
@Hive("test")
class SparkSQLParserTest extends SparkCore with SparkTester {
  val student = TableIdentifier("student")
  val baseorganize = TableIdentifier("dim.baseorganize")
  val baseuser = TableIdentifier("dim.baseuser")

  @Test
  @TestStep(step = 1, desc = "判断表属性")
  def testTable: Unit = {
    this.spark.createDataFrame(Student.newStudentList(), classOf[Student]).createOrReplaceTempView("student")

    println("student view: " + SparkSqlParser.isTempView(student))
    assert(SparkSqlParser.isTempView(student))
    println("student table: " + SparkSqlParser.isHiveTable(student))
    assert(!SparkSqlParser.isHiveTable(student))

    println("baseorganize view: " + SparkSqlParser.isTempView(baseorganize))
    assert(!SparkSqlParser.isTempView(baseorganize))
    println("baseorganize table: " + SparkSqlParser.isHiveTable(baseuser))
    assert(SparkSqlParser.isHiveTable(baseorganize))

    println("baseuser view: " + SparkSqlParser.isTempView(baseuser))
    assert(!SparkSqlParser.isTempView(baseuser))
    println("baseuser table: " + SparkSqlParser.isHiveTable(baseuser))
    assert(SparkSqlParser.isHiveTable(baseuser))
  }
}
