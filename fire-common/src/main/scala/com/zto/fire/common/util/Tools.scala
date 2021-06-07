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

package com.zto.fire.common.util

import com.zto.fire.common.ext.{JavaExt, ScalaExt}

import scala.collection.convert.{WrapAsJava, WrapAsScala}
import scala.util.control.Breaks

/**
 * 各种工具API的集合类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-16 16:23
 */
trait Tools extends Breaks with JavaTypeMap with ValueCheck with FireFunctions with JavaExt with ScalaExt with ScalaUtils with WrapAsScala with WrapAsJava {

}