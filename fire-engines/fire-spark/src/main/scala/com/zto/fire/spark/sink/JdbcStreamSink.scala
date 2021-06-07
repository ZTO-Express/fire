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

package com.zto.fire.spark.sink

import java.util.Objects

import com.zto.fire._
import com.zto.fire.jdbc.conf.FireJdbcConf
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

/**
 * jdbc sink组件，支持jdbc操作
 *
 * @param options
 * jdbc相关参数
 * @author ChengLong 2019年12月23日 13:06:30
 * @since 0.4.1
 */
class JdbcStreamSink(options: Map[String, String]) extends FireSink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println("latestBatchId=" + this.latestBatchId)
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val sql = options.getOrElse("sql", "")
      Objects.requireNonNull(sql, "sql语句不能为空.")
      val fields = options.getOrElse("fields", "")
      val batch = options.getOrElse("batch", FireJdbcConf.batchSize() + "").toInt
      val keyNum = options.getOrElse("keyNum", "1").toInt

      this.toExternalRow(data).jdbcBatchUpdate(sql, if (StringUtils.isNotBlank(fields)) fields.split(",") else null, batch, keyNum)
      latestBatchId = batchId
    }
  }
}