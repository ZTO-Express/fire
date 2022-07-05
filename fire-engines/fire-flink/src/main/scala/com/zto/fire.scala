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

package com.zto

import com.zto.fire.core.ext.BaseFireExt
import com.zto.fire.flink.ext.batch.{BatchExecutionEnvExt, BatchTableEnvExt, DataSetExt}
import com.zto.fire.flink.ext.function.{RichFunctionExt, RuntimeContextExt}
import com.zto.fire.flink.ext.stream._
import org.apache.flink.api.common.functions.{RichFunction, RuntimeContext}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, TableResult}
import org.apache.flink.types.Row

/**
 * 预定义fire框架中的扩展工具
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-22 13:51
 */
package object fire extends BaseFireExt {

  /**
   * StreamExecutionEnvironment扩展
   *
   * @param env
   * StreamExecutionEnvironment对象
   */
  implicit class StreamExecutionEnvExtBridge(env: StreamExecutionEnvironment) extends StreamExecutionEnvExt(env) {

  }

  /**
   * StreamTableEnvironment扩展
   *
   * @param tableEnv
   * StreamTableEnvironment对象
   */
  implicit class TableEnvExtBridge(tableEnv: TableEnvironment) extends TableEnvExt(tableEnv) {

  }


  /**
   * DataStream扩展
   *
   * @param dataStream
   * DataStream对象
   */
  implicit class DataStreamExtBridge[T](dataStream: DataStream[T]) extends DataStreamExt(dataStream) {

  }

  /**
   * KeyedStream扩展
   *
   * @param keyedStream
   * KeyedStream对象
   */
  implicit class KeyedStreamExtBridge[T, K](keyedStream: KeyedStream[T, K]) extends KeyedStreamExt[T, K](keyedStream) {

  }

  /**
   * TableResult扩展
   */
  implicit class TableResultImplBridge(tableResult: TableResult) extends TableResultImplExt(tableResult) {

  }

  /**
   * Table扩展
   *
   * @param table
   * Table对象
   */
  implicit class StreamTableExtBridge(table: Table) extends TableExt(table) {

  }

  /**
   * BatchTableEnvironment扩展
   *
   * @param tableEnv
   * BatchTableEnvironment对象
   */
  implicit class BatchTableEnvExtBridge(tableEnv: TableEnvironment) extends BatchTableEnvExt(tableEnv) {

  }


  /**
   * ExecutionEnvironment扩展
   *
   * @param env
   * ExecutionEnvironment对象
   */
  implicit class BatchExecutionEnvExtBridge(env: ExecutionEnvironment) extends BatchExecutionEnvExt(env) {

  }

  /**
   * DataSet扩展
   *
   * @param dataSet
   * DataSet对象
   */
  implicit class DataSetExtBridge[T](dataSet: DataSet[T]) extends DataSetExt(dataSet) {

  }

  /**
   * Row扩展
   */
  implicit class RowExtBridge(row: Row) extends RowExt(row) {

  }

  /**
   * Flink SQL扩展
   */
  implicit class SQLExtBridge(sql: String) extends SQLExt(sql) {

  }

  /**
   * Flink RuntimeContext扩展
   */
  implicit class RuntimeContextExtBridge(runtimeContext: RuntimeContext) extends RuntimeContextExt(runtimeContext) {

  }

  /**
   * Flink RichFunction扩展
   */
  implicit class RichFunctionExtBridge(richFunction: RichFunction) extends RichFunctionExt(richFunction) {

  }
}
