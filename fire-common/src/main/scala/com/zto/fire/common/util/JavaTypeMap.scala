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

/**
 * Java类型映射
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-16 15:40
 */
trait JavaTypeMap {
  // Java API库映射
  type JInt = java.lang.Integer
  type JLong = java.lang.Long
  type JBoolean = java.lang.Boolean
  type JChar = java.lang.Character
  type JFloat = java.lang.Float
  type JShort = java.lang.Short
  type JDouble = java.lang.Double
  type JBigDecimal = java.math.BigDecimal
  type JString = java.lang.String
  type JStringBuilder = java.lang.StringBuilder
  type JStringBuffer = java.lang.StringBuffer
  type JMap[K, V] = java.util.Map[K, V]
  type JHashMap[K, V] = java.util.HashMap[K, V]
  type JLinkedHashMap[K, V] = java.util.LinkedHashMap[K, V]
  type JConcurrentHashMap[K, V] = java.util.concurrent.ConcurrentHashMap[K, V]
  type JSet[E] = java.util.Set[E]
  type JHashSet[E] = java.util.HashSet[E]
  type JLinkedHashSet[E] = java.util.LinkedHashSet[E]
  type JList[E] = java.util.List[E]
  type JArrayList[E] = java.util.ArrayList[E]
  type JLinkedList[E] = java.util.LinkedList[E]
  type JQueue[E] = java.util.Queue[E]
  type JPriorityQueue[E] = java.util.PriorityQueue[E]
  type JCollections = java.util.Collections
}
