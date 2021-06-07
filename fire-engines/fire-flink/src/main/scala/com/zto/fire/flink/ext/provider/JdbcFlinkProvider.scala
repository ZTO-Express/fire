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

package com.zto.fire.flink.ext.provider

import com.zto.fire._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

/**
 * 为上层扩展层提供JDBC相关API
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-24 10:18
 */
trait JdbcFlinkProvider {

  /**
   * jdbc批量sink操作，根据用户指定的DataStream中字段的顺序，依次填充到sql中的占位符所对应的位置
   * 注：
   *  1. fieldList指定DataStream中JavaBean的字段名称，非jdbc表中的字段名称
   *  2. fieldList多个字段使用逗号分隔
   *  3. fieldList中的字段顺序要与sql中占位符顺序保持一致，数量一致
   *
   * @param sql
   * 增删改sql
   * @param fields
   * DataStream中数据的每一列的列名（非数据库中的列名，需与sql中占位符的顺序一致）
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def jdbcBatchUpdateStream[T](stream: DataStream[T],
                               sql: String,
                               fields: Seq[String],
                               batch: Int = 10,
                               flushInterval: Long = 1000,
                               keyNum: Int = 1): DataStreamSink[T] = {
    stream.jdbcBatchUpdate(sql, fields, batch, flushInterval, keyNum)
  }

  /**
   * jdbc批量sink操作
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  def jdbcBatchUpdateStream2[T](stream: DataStream[T],
                                sql: String,
                                batch: Int = 10,
                                flushInterval: Long = 1000,
                                keyNum: Int = 1)(fun: T => Seq[Any]): DataStreamSink[T] = {
    stream.jdbcBatchUpdate2(sql, batch, flushInterval, keyNum)(fun)
  }

  /**
   * table的jdbc批量sink操作，根据用户指定的Row中字段的顺序，依次填充到sql中的占位符所对应的位置
   * 注：
   *  1. Row中的字段顺序要与sql中占位符顺序保持一致，数量一致
   *  2. 目前仅处理Retract中的true消息，用户需手动传入merge语句
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def jdbcBatchUpdateTable(table: Table,
                           sql: String,
                           batch: Int = 10,
                           flushInterval: Long = 1000,
                           isMerge: Boolean = true,
                           keyNum: Int = 1): DataStreamSink[Row] = {
    table.jdbcBatchUpdate(sql, batch, flushInterval, isMerge, keyNum)
  }

  /**
   * table的jdbc批量sink操作，该api需用户定义row的取数规则，并与sql中的占位符对等
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def jdbcBatchUpdateTable2(table: Table,
                            sql: String,
                            batch: Int = 10,
                            flushInterval: Long = 1000,
                            isMerge: Boolean = true,
                            keyNum: Int = 1)(fun: Row => Seq[Any]): DataStreamSink[Row] = {
    table.jdbcBatchUpdate2(sql, batch, flushInterval, isMerge, keyNum)(fun)
  }

}
