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

import com.zto.fire._
import com.zto.fire.common.anno.TestStep
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.anno.connector.{Jdbc, Jdbc2}
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.core.SparkTester
import com.zto.fire.spark.SparkCore
import org.junit.Test

/**
 * Spark jdbc相关api单元测试
 *
 * @author ChengLong
 * @date 2022-05-12 13:49:24
 * @since 2.2.2
 */
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
@Jdbc2(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
class JdbcUnitTest extends SparkCore with SparkTester {
  lazy val tableName = "spark_test"

  /**
   * 使用jdbc方式对关系型数据库进行增删改操作
   */
  @Test
  @TestStep(step = 1, desc = "测试基本的增删改查api")
  def testCRUD: Unit = {
    this.truncate
    val timestamp = DateFormatUtils.formatCurrentDateTime()
    // 执行insert操作
    val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
    var resultList = this.fire.jdbcQueryList(s"select id, name, age, createTime, length, sex from $tableName where id=1", null, classOf[Student])
    assert(resultList.head.getName.equals("admin"))

    // 更新配置文件中指定的第二个关系型数据库
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1), keyNum = 2)
    resultList = this.fire.jdbcQueryList(s"select id, name, age, createTime, length, sex from $tableName where id=1", null, classOf[Student], keyNum = 2)
    assert(resultList.head.getName.equals("admin"))

    // 执行更新操作
    val updateSql = s"UPDATE $tableName SET name=? WHERE id=?"
    this.fire.jdbcUpdate(updateSql, Seq("root", 1))
    resultList = this.fire.jdbcQueryList(s"select id, name, age, createTime, length, sex from $tableName where id=1", null, classOf[Student])
    assert(resultList.head.getName.equals("root"))

    // 执行批量操作
    this.initData
    resultList = this.fire.jdbcQueryList(s"select id, name, age, createTime, length, sex from $tableName", null, classOf[Student])
    assert(resultList.size == 5)

    this.fire.jdbcBatchUpdate(s"update $tableName set sex=? where id=?", Seq(Seq(1, 1), Seq(2, 2), Seq(3, 3), Seq(4, 4), Seq(5, 5), Seq(6, 6)))
    val sql = s"DELETE FROM $tableName WHERE id=?"
    this.fire.jdbcUpdate(sql, Seq(2))
    resultList = this.fire.jdbcQueryList(s"select id, name, age, createTime, length, sex from $tableName where id=2", null, classOf[Student])
    assert(resultList.isEmpty)
  }

  /**
   * 使用jdbc方式对关系型数据库进行查询操作
   */
  @Test
  @TestStep(step = 2, desc = "测试查询相关的API")
  def testJdbcQuery: Unit = {
    this.initData
    val sql = s"select * from $tableName where id in (?, ?, ?)"

    // 将查询结果集以List[JavaBean]方式返回
    val list = this.fire.jdbcQueryList(sql, Seq(1, 2, 3), classOf[Student])
    // 方式二：使用JdbcConnector
    assert(list.size == 3)

    // 将结果集封装到RDD中
    val rdd = this.fire.jdbcQueryRDD(sql, Seq(1, 2, 3))
    assert(rdd.count() == 3)

    // 将结果集封装到DataFrame中
    val df = this.fire.jdbcQueryDF(sql, Seq(1, 2, 3))
    assert(df.count() == 3)
  }

  /**
   * 使用spark方式对表进行数据加载操作
   */
  @Test
  @TestStep(step = 3, desc = "测试基于spark的方式查询的数据结果")
  def testTableLoad: Unit = {
    this.initData
    // 一次加载整张的jdbc小表，注：大表严重不建议使用该方法
    val df = this.fire.jdbcTableLoadAll(this.tableName)
    assert(df.count() == 5)
    // 根据指定分区字段的上下边界分布式加载数据
    this.fire.jdbcTableLoadBound(this.tableName, "id", 1, 10, 2).show(100, false)
    val where = Array[String]("id >=1 and id <=3", "id >=6 and id <=9", "name='root'")
    // 根据指定的条件进行数据加载，条件的个数决定了load数据的并发度
    val df2 = this.fire.jdbcTableLoad(tableName, where)
    assert(df2.count() == 3)
  }

  /**
   * 批量插入测试数据
   */
  private def initData: Unit = {
    this.truncate
    val timestamp = DateFormatUtils.formatCurrentDateTime()
    // 执行批量操作
    val batchSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"

    this.fire.jdbcBatchUpdate(batchSql, Seq(Seq("spark1", 21, timestamp, 100.123, 1),
      Seq("flink2", 22, timestamp, 12.236, 0),
      Seq("flink3", 22, timestamp, 12.236, 0),
      Seq("flink4", 22, timestamp, 12.236, 0),
      Seq("flink5", 27, timestamp, 17.236, 0)))
  }

  /**
   * 清空表
   */
  private def truncate: Unit = {
    this.fire.jdbcUpdate(s"truncate table $tableName")
  }
}