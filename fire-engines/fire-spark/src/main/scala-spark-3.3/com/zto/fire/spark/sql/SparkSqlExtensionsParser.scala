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

package com.zto.fire.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Spark Sql解析扩展，用于拦截执行的sql以及解析sql中的血缘
 *
 * @author ChengLong 2021-6-23 10:25:17
 * @since 2.0.0
 */
private[fire] class SparkSqlExtensionsParser(sparkSession: SparkSession, parser: ParserInterface)
  extends SparkSqlExtensionsParserBase(sparkSession, parser) with ParserInterface {

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    parser.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parser.parseFunctionIdentifier(sqlText)
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    parser.parseMultipartIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    parser.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    parser.parseDataType(sqlText)
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    parser.parseQuery(sqlText)
  }
}