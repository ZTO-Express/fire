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

package com.zto.fire.flink.ext.batch

import com.zto.fire.flink.util.FlinkSingletonFactory
import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
 * 用于对Flink DataSet的API库扩展
 *
 * @author ChengLong 2020年1月15日 16:35:03
 * @since 0.4.1
 */
class DataSetExt[T](dataSet: DataSet[T]){
  lazy val tableEnv = FlinkSingletonFactory.getTableEnv.asInstanceOf[TableEnvironment]

  /**
   * 将DataSet注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    val table = this.tableEnv.fromValues(this.dataSet)
    this.tableEnv.createTemporaryView(tableName, table)
    table
  }

  /**
   * 设置并行度
   */
  def repartition(parallelism: Int): DataSet[T] = {
    this.dataSet.setParallelism(parallelism)
  }

  /**
   * 将DataSet转为Table
   */
  def toTable: Table = {
    this.tableEnv.fromValues(this.dataSet)
  }


}
