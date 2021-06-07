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

import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

/**
 * Fire框架组件sink父类
 *
 * @author ChengLong 2019年12月23日 10:09:55
 * @since 0.4.1
 */
private[fire] abstract class FireSink extends Sink with Logging {
  @volatile protected var latestBatchId = -1L
  protected lazy val spark = SparkSingletonFactory.getSparkSession

  /**
   * 将内部row类型的DataFrame转为Row类型的DataFrame
   *
   * @param df
   * InternalRow类型的DataFrame
   * @return
   * Row类型的DataFrame
   */
  protected def toExternalRow(df: DataFrame): DataFrame = {
    SparkUtils.toExternalRow(df)
  }
}