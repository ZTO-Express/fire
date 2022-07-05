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

package com.zto.fire.examples.spark.hbase

/**
  * Hive sql
 *
  * @author ChengLong 2019-1-16 09:53:45
  * @contact Fire框架技术交流群（钉钉）：35373471
  */
object HiveQL {

  /**
    * 执行order main sql
    * @param tableName
    * @return
    */
  def saveMainOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |after.*,
       |before.bill_code before_bill_code,
       |before.order_code before_order_code
       |from ${tableName}
       |where op_type<>'D'
       |and after.bill_code<>''
       |and substr(table,0,6)='order_'
       |and substr(table,0,7)<>'order_r'
      """.stripMargin
  }

  /**
    * 执行delete order main sql
    * @param tableName
    * @return
    */
  def deleteMainOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |before.*
       |from ${tableName}
       |where op_type='D'
       |and before.bill_code<>''
       |and before.order_create_date>'2018-06-01'
       |and substr(table,0,6)='order_'
       |and substr(table,0,7)<>'order_r'
      """.stripMargin
  }

  /**
    * 执行save replica order sql
    * @param tableName
    * @return
    */
  def saveReplicaOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |after.*,
       |before.bill_code before_bill_code,
       |before.order_code before_order_code
       |from ${tableName}
       |where op_type<>'D'
       |and after.bill_code<>''
       |and substr(table,0,7)='order_r'
      """.stripMargin
  }

  /**
    * 执行delete replica order sql
    * @param tableName
    * @return
    */
  def deleteReplicaOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |before.*
       |from ${tableName}
       |where op_type='D'
       |and before.order_create_date>'2018-06-01'
       |and before.bill_code<>''
       |and substr(table,0,7)='order_r'
      """.stripMargin
  }
}
