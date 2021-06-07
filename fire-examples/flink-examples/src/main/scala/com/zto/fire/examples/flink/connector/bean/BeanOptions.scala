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

import com.zto.fire.{JInt, JLong}
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}

/**
 * 自定义sql connector支持的选项
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
object BeanOptions {
  val TABLE_NAME: ConfigOption[String] = ConfigOptions
    .key("table-name")
    .stringType
    .noDefaultValue
    .withDescription("The name of impala table to connect.")

  val DURATION: ConfigOption[JLong] = ConfigOptions
    .key("duration")
    .longType()
    .defaultValue(3000L)
    .withDescription("The duration of data send.")

  val repeatTimes: ConfigOption[JInt] = ConfigOptions
    .key("repeat-times")
    .intType()
    .defaultValue(5)
    .withDescription("The repeat times.")
}
