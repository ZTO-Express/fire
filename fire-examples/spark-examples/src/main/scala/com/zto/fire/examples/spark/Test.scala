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

package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.common.util.{DateFormatUtils, PropUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.examples.spark.jdbc.JdbcTest.tableName
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.{BaseSparkCore, BaseSparkStreaming}


/**
 * 基于Fire进行Spark Streaming开发
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-06-07 13:14:19
 */
object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print()
    this.fire.start
  }
}