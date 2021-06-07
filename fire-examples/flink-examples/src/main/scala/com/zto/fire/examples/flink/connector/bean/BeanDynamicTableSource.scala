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

import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.examples.bean.People
import com.zto.fire.flink.util.FlinkUtils
import com.zto.fire.predef._
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

/**
 * 定义source table
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class BeanDynamicTableSource(tableSchema: TableSchema, options: ReadableConfig, producedDataType: DataType) extends ScanTableSource {

  override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()

  override def copy(): DynamicTableSource = new BeanDynamicTableSource(tableSchema, options, producedDataType)

  override def asSummaryString(): String = "bean"

  /**
   * 核心逻辑，定义如何产生source表的数据
   */
  override def getScanRuntimeProvider(scanContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    // source table的schema
    val rowType = this.tableSchema.toRowDataType.getLogicalType.asInstanceOf[RowType]
    // 将自定义的source function传入
    SourceFunctionProvider.of(new BeanSourceFunction(rowType, options), false)
  }

}

/**
 * 自定义的sink function，用于通知flink sql，如何将RowData数据收集起来
 */
class BeanSourceFunction(rowType: RowType, options: ReadableConfig) extends RichSourceFunction[RowData] {

  override def run(ctx: SourceFunction.SourceContext[RowData]): Unit = {
    // 指定每次sink多久以后进行下一次的sink
    val duration = options.get(BeanOptions.DURATION)
    // 获取配置的重复次数，指定重发几次
    val times = options.get(BeanOptions.repeatTimes)
    for (i <- 1 to times) {
      People.createList().foreach(people => {
        // 通过ctx收集sink的数据
        ctx.collect(FlinkUtils.bean2RowData(people, rowType))
      })
      println(s"================${DateFormatUtils.formatCurrentDateTime()}==================")
      Thread.sleep(duration)
    }
  }

  override def cancel(): Unit = {}

}