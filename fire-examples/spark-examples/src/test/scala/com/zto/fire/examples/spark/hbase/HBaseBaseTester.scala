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

package com.zto.fire.examples.spark.hbase

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.core.BaseSparkTester
import com.zto.fire.hbase.HBaseConnector
import org.junit.Before

/**
 * 用于对HBaseAPI进行单元测试的工具trait
 *
 * @author ChengLong
 * @date 2022-05-11 13:52:25
 * @since 2.2.2
 */
trait HBaseBaseTester extends BaseSparkTester {
  val tableName1 = "fire_test_1"
  val tableName2 = "fire_test_2"

  @Before
  override def before: Unit = {
    super.before
    if (!HBaseConnector.isExists(this.tableName1)) HBaseConnector.createTable(this.tableName1, Seq("info"))
    if (!HBaseConnector.isExists(this.tableName2)) HBaseConnector.createTable(this.tableName2, Seq("info"))
    this.truncate
  }

  /**
   * 向HBase中插入数据
   */
  protected[this] def putData: Unit = {
    this.truncate
    val studentList = Student.newStudentList()
    this.fire.hbasePutList(this.tableName1, studentList)
  }

  protected[this] def truncate: Unit = {
    HBaseConnector.truncateTable(this.tableName1)
    HBaseConnector.truncateTable(this.tableName2, keyNum = 2)
  }
}
