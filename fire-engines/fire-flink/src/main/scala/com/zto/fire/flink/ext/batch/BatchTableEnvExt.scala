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

import com.zto.fire.jdbc.JdbcConnectorBridge
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

/**
 * 用于flink BatchTableEnvironment API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
class BatchTableEnvExt(env: BatchTableEnvironment) extends JdbcConnectorBridge {

  /**
   * 执行sql query操作
   *
   * @param sql
   * sql语句
   * @return
   * table对象
   */
  def sql(sql: String): Table = {
    this.env.sqlQuery(sql)
  }

}
