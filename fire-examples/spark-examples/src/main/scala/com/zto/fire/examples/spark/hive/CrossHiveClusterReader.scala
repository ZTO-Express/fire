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

package com.zto.fire.examples.spark.hive

import com.zto.fire._
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.spark.BaseSparkCore

object CrossHiveClusterReader extends BaseSparkCore {
  val dfsUrl = "hdfs://192.168.25.37:8020/user/hive/warehouse/ba.db/one_two_disp_dm"

  def main(args: Array[String]): Unit = {
    this.init()
    var startTime = DateFormatUtils.currentTime
    val sendaDF = this.hiveContext.read.option("header", "true").option("inferSchema", "true")
      .format("orc")
      .load(this.dfsUrl)
    sendaDF.createOrReplaceTempView("tmp1")
    this.fire.sql("select count(1) from tmp1 where ds>=20190315").show()
    println(DateFormatUtils.runTime(startTime))

    startTime = DateFormatUtils.currentTime
    val sendaDF2 = this.hiveContext.read.option("header", "true")
      .option("inferSchema", "true")
      .format("orc").load(
      s"${this.dfsUrl}/ds=20190315",
      s"${this.dfsUrl}/ds=20190316",
      s"${this.dfsUrl}/ds=20190317",
      s"${this.dfsUrl}/ds=20190318",
      s"${this.dfsUrl}/ds=20190319",
      s"${this.dfsUrl}/ds=20190320",
      s"${this.dfsUrl}/ds=20190321",
      s"${this.dfsUrl}/ds=20190322"
    )
    sendaDF2.createOrReplaceTempView("tmp2")
    this.fire.sql("select count(1) from tmp2").show()
    println(DateFormatUtils.runTime(startTime))
  }
}
