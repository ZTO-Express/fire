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

package com.zto.fire.jdbc

import com.zto.fire.common.anno.TestStep
import com.zto.fire.common.db.bean.Student
import com.zto.fire.common.util.{DatasourceManager, PropUtils}
import com.zto.fire.predef._
import org.junit.Assert._
import org.junit.{After, Before, Test}

/**
 * 用于测试JdbcConnector相关API
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-30 14:23
 */
class JdbcConnectorTest {
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
  def init: Unit = {
    PropUtils.load("JdbcConnectorTest")
    this.jdbc = JdbcConnector()
    this.jdbc.executeUpdate(this.createTable)
    this.jdbc3 = JdbcConnector(keyNum = 3)
    this.jdbc3.executeUpdate(this.createTable)
  }


  @Test
  @TestStep(step = 1, desc = "jdbc CRUD测试")
  def testCRUD: Unit = {
    val studentName = "root"

    val deleteSql = s"delete from $tableName where name=?"
    this.jdbc.executeUpdate(deleteSql, Seq(studentName))
    this.jdbc3.executeUpdate(deleteSql, Seq(studentName))

    val selectSql = s"select * from $tableName where name=?"
    val studentList1 = this.jdbc.executeQuery(selectSql, Seq(studentName), classOf[Student])
    val studentList3 = this.jdbc3.executeQuery(selectSql, Seq(studentName), classOf[Student])
    assertEquals(studentList1.size, 0)
    studentList1.foreach(println)
    assertEquals(studentList3.size, 0)
    studentList3.foreach(println)

    val insertSql = s"insert into $tableName(name, age, length) values(?, ?, ?)"
    this.jdbc.executeUpdate(insertSql, Seq(studentName, 10, 10.3))
    this.jdbc3.executeUpdate(insertSql, Seq(studentName, 10, 10.3))

    val studentList11 = this.jdbc.executeQuery(selectSql, Seq(studentName), classOf[Student])
    val studentList33 = this.jdbc3.executeQuery(selectSql, Seq(studentName), classOf[Student])
    assertEquals(studentList11.size, 1)
    studentList11.foreach(println)
    assertEquals(studentList33.size, 1)
    studentList33.foreach(println)

    for (i <- 1 to 5) {
      DatasourceManager.get.foreach(t => {
        t._2.foreach(source => {
          println("数据源：" + t._1.toString + " " + source)
        })
      })
      println("=====================================")
      Thread.sleep(1000)
    }
  }

  @After
  def close: Unit = {
    this.jdbc.executeUpdate(s"drop table $tableName")
    this.jdbc3.executeUpdate(s"drop table $tableName")
  }
}
