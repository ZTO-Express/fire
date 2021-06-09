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
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.spark.BaseSparkCore
import com.zto.fire.spark.util.SparkUtils
import org.apache.spark.sql.SaveMode

/**
 * Spark jdbc操作
 *
 * @author ChengLong 2019-6-17 15:17:38
 */
object JdbcTest extends BaseSparkCore {
  lazy val tableName = "spark_test"
  lazy val tableName2 = "t_cluster_info"
  lazy val tableName3 = "t_cluster_status"

  /**
   * 使用jdbc方式对关系型数据库进行增删改操作
   */
  def testJdbcUpdate: Unit = {
    val timestamp = DateFormatUtils.formatCurrentDateTime()
    // 执行insert操作
    val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
    // 更新配置文件中指定的第二个关系型数据库
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1), keyNum = 2)

    // 执行更新操作
    val updateSql = s"UPDATE $tableName SET name=? WHERE id=?"
    this.fire.jdbcUpdate(updateSql, Seq("root", 1))

    // 执行批量操作
    val batchSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"

    this.fire.jdbcBatchUpdate(batchSql, Seq(Seq("spark1", 21, timestamp, 100.123, 1),
      Seq("flink2", 22, timestamp, 12.236, 0),
      Seq("flink3", 22, timestamp, 12.236, 0),
      Seq("flink4", 22, timestamp, 12.236, 0),
      Seq("flink5", 27, timestamp, 17.236, 0)))

    // 执行批量更新
    this.fire.jdbcBatchUpdate(s"update $tableName set sex=? where id=?", Seq(Seq(1, 1), Seq(2, 2), Seq(3, 3), Seq(4, 4), Seq(5, 5), Seq(6, 6)))

    // 方式一：通过this.fire方式执行delete操作
    val sql = s"DELETE FROM $tableName WHERE id=?"
    this.fire.jdbcUpdate(sql, Seq(2))
    // 方式二：通过JdbcConnector.executeUpdate

    // 同一个事务
    /*val connection = this.jdbc.getConnection()
    this.fire.jdbcBatchUpdate("insert", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("delete", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("update", connection = connection, commit = true, closeConnection = true)*/
  }


  /**
   * 使用jdbc方式对关系型数据库进行查询操作
   */
  def testJdbcQuery: Unit = {
    val sql = s"select * from $tableName where id in (?, ?, ?)"

    // 执行sql查询，并对查询结果集进行处理
    this.fire.jdbcQueryCall(sql, Seq(1, 2, 3), callback = rs => {
      while (rs.next()) {
        // 对每条记录进行处理
        println("driver=> id=" + rs.getLong(1))
      }
      1
    })

    // 将查询结果集以List[JavaBean]方式返回
    val list = this.fire.jdbcQuery(sql, Seq(1, 2, 3), classOf[Student])
    // 方式二：使用JdbcConnector
    list.foreach(x => println(JSONUtils.toJSONString(x)))

    // 将结果集封装到RDD中
    val rdd = this.fire.jdbcQueryRDD(sql, Seq(1, 2, 3), classOf[Student])
    rdd.printEachPartition

    // 将结果集封装到DataFrame中
    val df = this.fire.jdbcQueryDF(sql, Seq(1, 2, 3), classOf[Student])
    df.show(10, false)

    // 将jdbc查询结果集封装到Dataset中
    val ds = this.fire.jdbcQueryDS(sql, Seq(1, 2, 3), classOf[Student])
    ds.show(10, false)
  }

  /**
   * 使用spark方式对表进行数据加载操作
   */
  def testTableLoad: Unit = {
    // 一次加载整张的jdbc小表，注：大表严重不建议使用该方法
    this.fire.jdbcTableLoadAll(this.tableName).show(100, false)
    // 根据指定分区字段的上下边界分布式加载数据
    this.fire.jdbcTableLoadBound(this.tableName, "id", 1, 10, 2).show(100, false)
    val where = Array[String]("id >=1 and id <=3", "id >=6 and id <=9", "name='root'")
    // 根据指定的条件进行数据加载，条件的个数决定了load数据的并发度
    this.fire.jdbcTableLoad(tableName, where).show(100, false)
  }

  /**
   * 使用spark方式批量写入DataFrame数据到关系型数据库
   */
  def testTableSave: Unit = {
    // 批量将DataFrame数据写入到对应结构的关系型表中
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    // 第二个参数默认为SaveMode.Append，可以指定SaveMode.Overwrite
    df.jdbcTableSave(this.tableName, SaveMode.Overwrite)
    // 利用sparkSession方式将DataFrame数据保存到配置的第二个数据源中
    this.fire.jdbcTableSave(df, this.tableName, SaveMode.Overwrite)
  }

  /**
   * 将DataFrame数据写入到关系型数据库中
   */
  def testDataFrameSave: Unit = {
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])

    val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    // 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序，batch用于指定批次大小，默认取spark.db.jdbc.batch.size配置的值
    df.jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 100)

    df.createOrReplaceTempViewCache("student")
    val sqlDF = this.fire.sql("select name, age, createTime from student where id>=1").repartition(1)
    // 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
    sqlDF.jdbcBatchUpdate("insert into spark_test(name, age, createTime) values(?, ?, ?)")
    // 等同以上方式
    // this.fire.jdbcBatchUpdateDF(sqlDF, "insert into spark_test(name, age, createTime) values(?, ?, ?)")
  }

  /**
   * 在executor中执行jdbc操作
   */
  def testExecutor: Unit = {
    JdbcConnector.executeQueryCall(s"select id from $tableName limit 1", null, callback = _ => {
      // this.mark()
      Thread.sleep(1000)
      // this.log(s"=============driver123 $tableName2=============")
      1
    })
    JdbcConnector.executeQueryCall(s"select id from $tableName limit 1", null, callback = _ => {
      // this.log(s"=============driver $tableName2=============")
      1
    }, keyNum = 2)
    this.logger.info("driver sql执行成功")
    val rdd = this.fire.createRDD(1 to 3, 3)
    rdd.foreachPartition(it => {
      it.foreach(i => {
        JdbcConnector.executeQueryCall(s"select id from $tableName limit 1", null, callback = _ => {
          // this.log("------------------------- executorId: " + SparkUtils.getExecutorId + " date:" + DateFormatUtils.formatCurrentDate())
          1
        })
      })
      this.logger.info("sql执行成功")
    })

    this.logConf
    val rdd2 = this.fire.createRDD(1 to 3, 3)
    rdd2.foreachPartition(it => {
      it.foreach(i => {
        JdbcConnector.executeQueryCall(s"select id from $tableName limit 1", null, callback = _ => {
          this.logConf
          1
        }, keyNum = 2)
        this.logger.info("sql执行成功")
      })
    })
  }

  /**
   * 用于测试分布式配置
   */
  def logConf: Unit = {
    println(s"executorId=${SparkUtils.getExecutorId} hello.world=" + this.conf.getString("hello.world", "not_found"))
    println(s"executorId=${SparkUtils.getExecutorId} hello.world.flag=" + this.conf.getBoolean("hello.world.flag", false))
    println(s"executorId=${SparkUtils.getExecutorId} hello.world.flag2=" + this.conf.getBoolean("hello.world.flag", false, keyNum = 2))
  }

  override def process: Unit = {
    // 测试环境测试
    this.testJdbcUpdate
    this.testJdbcQuery
    this.testTableLoad
    this.testTableSave
    this.testDataFrameSave
    // 测试配置分发
    this.testExecutor
  }

  override def main(args: Array[String]): Unit = {
    this.init(args = args)

    Thread.currentThread().join()
  }
}