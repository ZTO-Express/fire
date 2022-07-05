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

import com.zto.fire.common.anno.TestStep
import com.zto.fire.core.anno.Hive
import com.zto.fire.examples.spark.core.BaseSparkTester
import com.zto.fire.spark.BaseSparkCore
import org.junit.Test

/**
 * 用于测试与hive的集成
 *
 * @author ChengLong
 * @date 2022-05-12 14:56:36
 * @since 2.2.2
 */
@Hive("test")
class HiveUnitTest extends BaseSparkCore with BaseSparkTester {

  @Test
  @TestStep(step = 1, desc = "测试列出所有的数据库名称")
  def testShowDatabases: Unit = {
    val df = this.fire.sql("show databases")
    assert(df.count() > 3)
  }

  @Test
  @TestStep(step = 1, desc = "测试列出tmp库下所有的hive表名称")
  def testShowTables: Unit = {
    this.fire.sql("use tmp")
    val df = this.fire.sql("show tables")
    assert(df.count() > 10)
  }
}
