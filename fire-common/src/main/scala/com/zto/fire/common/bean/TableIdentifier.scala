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

package com.zto.fire.common.bean

import com.zto.fire.predef._

/**
 * 用于标识表的信息
 *
 * @author ChengLong 2022-09-06 15:19:55
 * @since 2.3.2
 */
case class TableIdentifier(private val _table: String, private val _database: String = "") {

  lazy val table = {
    if (this._table.contains(".")) {
      this._table.split('.')(1)
    } else this._table
  }

  lazy val database = {
    if (isEmpty(this._database) && this._table.contains(".")) {
      this._table.split('.')(0)
    } else this._database
  }

  /**
   * 用于判断是否存在数据库名称
   * 如果将库名直接写到表名中，也认为库存在
   */
  def existsDB: Boolean = noEmpty(this.database) || table.contains(".")

  def notExistsDB: Boolean = !this.existsDB

  /**
   * 获取库表描述信息
   */
  def identifier: String = this.toString

  override def toString: JString = {
    if (noEmpty(database)) s"$database.$table".trim else table.trim
  }
}
