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

import scala.reflect.{ClassTag, classTag}
import scala.runtime.Nothing$

/**
 * scala工具类
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-04 14:06
 */
trait ScalaUtils {

  /**
   * 获取泛型具体的类型
   *
   * @tparam T
   * 泛型类型
   * @return
   * Class[T]
   */
  def getParamType[T: ClassTag]: Class[T] = {
    val paramType = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    if (paramType == classOf[Nothing$]) throw new IllegalArgumentException("不合法的方法调用，请在方法调用时指定泛型！")
    paramType
  }

  /**
   * 用于判断给定的类是否为object
   * @return
   * true：对象或半生对象 false：class
   */
  def isObject(clazz: Class[_]): Boolean = clazz.getName.endsWith("$")
}
