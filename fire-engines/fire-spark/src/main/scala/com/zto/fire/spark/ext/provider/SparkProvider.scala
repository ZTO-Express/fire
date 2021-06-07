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

package com.zto.fire.spark.ext.provider

import com.zto.fire.core.ext.Provider
import com.zto.fire.spark.util.SparkSingletonFactory

/**
 * spark provider父接口
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:49
 */
trait SparkProvider extends Provider {
  protected lazy val spark = SparkSingletonFactory.getSparkSession
  protected lazy val sc = spark.sparkContext
}
