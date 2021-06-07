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

package com.zto.fire.core.rest

import spark.{Request, Response}

/**
  * 用于封装rest的相关信息
  *
  * @param method
  * rest的提交方式：GET/POST/PUT/DELETE等
  * @param path
  * rest服务地址
  * @author ChengLong 2019-3-16 09:58:06
  */
private[fire] case class RestCase(method: String, path: String, fun: (Request, Response) => AnyRef)
