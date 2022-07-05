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

package com.zto.fire.examples.spark.jdbc

import com.zto.fire.common.anno.TestStep
import com.zto.fire.common.util.{DatasourceManager, PropUtils}
import com.zto.fire.core.anno.{Jdbc, Jdbc3}
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.core.BaseSparkTester
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.predef._
import com.zto.fire.spark.BaseSparkCore
import org.junit.Assert._
import org.junit.{After, Before, Test}

/**
 * 用于测试JdbcConnector相关API
 *
 * @author ChengLong
 * @since 2.2.2
 * @create 2022-05-12 13:26:11
 */
@Jdbc(url = "jdbc:derby:memory:fire;create=true", username = "fire", password = "fire", driver = "org.apache.derby.jdbc.EmbeddedDriver")
@Jdbc3(url = "jdbc:derby:memory:fire2;create=true", username = "fire", password = "fire", maxPoolSize = 1, driver = "org.apache.derby.jdbc.EmbeddedDriver")
class JdbcConnectorTest extends BaseSparkCore with BaseSparkTester {
  private var jdbc: JdbcConnector = _
  private var jdbc3: JdbcConnector = _
  private val tableName = "t_student"

  private val createTable =
    s"""
      |CREATE TABLE $tableName(
      |	id BIGINT,
      |	name VARCHAR(100),
      |	age INT,
      |	createTime VARCHAR(20),
      |	length double,
      |	sex CHAR,
      |	rowkey VARCHAR(100)
      |)
      |""".stripMargin

  @Before
  override def before: Unit = {
    super.before
    this.jdbc = JdbcConnector()
    this.jdbc.executeUpdate(this.createTable)
    this.jdbc3 = JdbcConnector(keyNum = 3)
    this.jdbc3.executeUpdate(this.createTable)
  }

  /**
   * 基于derby数据库进行crud测试
   */
  @Test
  @TestStep(step = 1, desc = "jdbc CRUD测试")
  def testCRUD: Unit = {
    val studentName = "root"

    val deleteSql = s"delete from $tableName where name=?"
    this.jdbc.executeUpdate(deleteSql, Seq(studentName))
    this.jdbc3.executeUpdate(deleteSql, Seq(studentName))

    val selectSql = s"select * from $tableName where name=?"
    val studentList1 = this.jdbc.executeQueryList(selectSql, Seq(studentName), classOf[Student])
    val studentList3 = this.jdbc3.executeQueryList(selectSql, Seq(studentName), classOf[Student])
    assertEquals(studentList1.size, 0)
    studentList1.foreach(println)
    assertEquals(studentList3.size, 0)
    studentList3.foreach(println)

    val insertSql = s"insert into $tableName(name, age, length) values(?, ?, ?)"
    this.jdbc.executeUpdate(insertSql, Seq(studentName, 10, 10.3))
    this.jdbc3.executeUpdate(insertSql, Seq(studentName, 10, 10.3))

    val studentList11 = this.jdbc.executeQueryList(selectSql, Seq(studentName), classOf[Student])
    val studentList33 = this.jdbc3.executeQueryList(selectSql, Seq(studentName), classOf[Student])
    assertEquals(studentList11.size, 1)
    studentList11.foreach(println)
    assertEquals(studentList33.size, 1)
    studentList33.foreach(println)
  }

  @After
  override def after: Unit = {
    this.jdbc.executeUpdate(s"drop table $tableName")
    this.jdbc3.executeUpdate(s"drop table $tableName")
  }
}
