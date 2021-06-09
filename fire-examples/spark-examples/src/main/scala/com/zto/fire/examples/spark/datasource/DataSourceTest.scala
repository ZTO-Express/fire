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

package com.zto.fire.examples.spark.datasource

import com.zto.fire._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkCore
import org.apache.spark.sql.SaveMode


/**
 * Spark DataSource API示例
 */
object DataSourceTest extends BaseSparkCore {

  override def process: Unit = {
    val ds = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    ds.createOrReplaceTempView("test")

    val dataFrame = this.fire.sql("select * from test")

    // 一、 dataFrame.write.format.mode.save中的所有参数均可通过配置文件指定
    // dataFrame.writeEnhance()

    // 二、 dataFrame.write.mode.save中部分参数通过配置文件指定，或全部通过方法硬编码指定
    val savePath = "/user/hive/warehouse/hudi.db/hudi_bill_event_test"

    // 如果代码中与配置文件中均指定了options，则相同的options配置文件优先级更高，不同的option均生效
    val options = Map(
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombine.field" -> "id"
    )

    // 使用keyNum标识读取配置文件中不同配置后缀的options信息
    // dataFrame.writeEnhance("org.apache.hudi", SaveMode.Append, savePath, options = options, keyNum = 2)

    // read.format.mode.load(path)
    this.fire.readEnhance(keyNum = 3)
  }
}
