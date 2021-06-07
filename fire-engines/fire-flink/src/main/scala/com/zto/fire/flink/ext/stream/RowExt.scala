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

package com.zto.fire.flink.ext.stream

import com.zto.fire.flink.bean.FlinkTableSchema
import com.zto.fire.flink.util.FlinkUtils
import org.apache.flink.types.Row

/**
 * 用于flink Row API库扩展
 *
 * @author ChengLong 2020年3月30日 17:00:05
 * @since 0.4.1
 */
class RowExt(row: Row) {

  /**
   * 将flink的row转为指定类型的JavaBean
   * @param schema
   *               表的schema
   * @param clazz
   *              目标JavaBean类型
   */
  def rowToBean[T](schema: FlinkTableSchema, clazz: Class[T]): T = {
    FlinkUtils.rowToBean(schema, row, clazz)
  }
}
