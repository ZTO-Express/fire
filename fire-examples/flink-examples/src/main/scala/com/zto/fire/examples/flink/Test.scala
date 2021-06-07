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

package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.common.util.{JSONUtils, PropUtils, StringsUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.catalog.ObjectPath

/**
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
object Test extends BaseFlinkStreaming {

  override def process: Unit = {
    this.tableEnv.useCatalog(FireHiveConf.hiveCatalogName)
    this.tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    this.fire.sql(
      """
        |insert into hive.tmp.fire select * from tmp.account
        |""".stripMargin)
    this.fire.sql(
      """
        |select * from tmp.fire
        |""".stripMargin).print()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
