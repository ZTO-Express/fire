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

package com.zto.fire.examples.flink.connector.bean

import com.zto.fire.predef._
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType

/**
 * sql connector的sink
 * @author ChengLong 2021-5-7 15:48:03
 */
class BeanDynamicTableSink(tableSchema: TableSchema, options: ReadableConfig, dataType: DataType) extends DynamicTableSink {
  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = ChangelogMode.insertOnly()

  override def copy(): DynamicTableSink = new BeanDynamicTableSink(tableSchema, options, dataType)

  override def asSummaryString(): JString = "bean-sink"

  /**
   * 核心逻辑，定义如何将数据sink
   */
  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    SinkFunctionProvider.of(new RichSinkFunction[RowData] {
      override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
        println("sink---> " + value.toString)
      }
    })
  }
}