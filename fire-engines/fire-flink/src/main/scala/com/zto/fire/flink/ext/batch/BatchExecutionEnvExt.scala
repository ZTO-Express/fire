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

package com.zto.fire.flink.ext.batch

import com.zto.fire.common.util.ValueUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.reflect.ClassTag

/**
 * 用于flink ExecutionEnvironment API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
class BatchExecutionEnvExt(env: ExecutionEnvironment) {

  /**
   * 提交job执行
   *
   * @param jobName
   * job名称
   */
  def start(jobName: String = ""): Unit = {
    if (ValueUtils.isEmpty(jobName)) this.env.execute() else this.env.execute(jobName)
  }

  /**
   * 使用集合元素创建DataStream
   * @param seq
   *            元素集合
   * @tparam T
   *           元素的类型
   */
  def parallelize[T: TypeInformation: ClassTag](seq: Seq[T], parallelism: Int = this.env.getParallelism): DataSet[T] = {
    this.env.fromCollection[T](seq).setParallelism(parallelism)
  }

  /**
   * 使用集合元素创建DataStream
   * @param seq
   *            元素集合
   * @tparam T
   *           元素的类型
   */
  def createCollectionDataSet[T: TypeInformation: ClassTag](seq: Seq[T], parallelism: Int = this.env.getParallelism): DataSet[T] = this.parallelize[T](seq, parallelism)
}
