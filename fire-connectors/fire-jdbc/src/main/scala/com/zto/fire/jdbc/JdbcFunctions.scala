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

import java.sql.{Connection, ResultSet}

import scala.reflect.ClassTag

/**
 * Jdbc api集合
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 15:49
 */
private[fire] trait JdbcFunctions {

  /**
   * 根据指定的keyNum获取对应的数据库连接
   */
  def getConnection(keyNum: Int = 1): Connection = JdbcConnector(keyNum = keyNum).getConnection

  /**
   * 更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param params
   * sql中的参数
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param closeConnection
   * 是否关闭connection，默认关闭
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 影响的记录数
   */
  def executeUpdate(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Long = {
    JdbcConnector(keyNum = keyNum).executeUpdate(sql, params, connection, commit, closeConnection)
  }

  /**
   * 执行批量更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param paramsList
   * sql的参数列表
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param closeConnection
   * 是否关闭connection，默认关闭
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 影响的记录数
   */
  def executeBatch(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Array[Int] = {
    JdbcConnector(keyNum = keyNum).executeBatch(sql, paramsList, connection, commit, closeConnection)
  }

  /**
   * 执行查询操作，以JavaBean方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param clazz
   * JavaBean类型
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def executeQuery[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null, keyNum: Int = 1): List[T] = {
    JdbcConnector(keyNum = keyNum).executeQuery(sql, params, clazz, connection)
  }

  /**
   * 执行查询操作
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param callback
   * 查询回调
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def executeQueryCall(sql: String, params: Seq[Any] = null, callback: ResultSet => Int = null, connection: Connection = null, keyNum: Int = 1): Unit = {
    JdbcConnector(keyNum = keyNum).executeQueryCall(sql, params, callback, connection)
  }
}
