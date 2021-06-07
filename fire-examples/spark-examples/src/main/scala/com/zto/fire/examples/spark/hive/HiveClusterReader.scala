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

package com.zto.fire.examples.spark.hive

import com.zto.fire.spark.BaseSparkCore

/**
  * 本示例用于演示spark读取不同hive集群，配置文件请见 HiveClusterReader.properties，继承自BaseSparkCore表示是一个离线的spark程序
  * 如果需要使用不同的hive集群，只需在该类同名的配置文件中加一下配置即可：hive.cluster=streaming，表示读取180实时集群的hive元数据
  *
  * @author ChengLong 2019-5-17 10:39:19
  */
object HiveClusterReader extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    // 必须调用init()方法完成sparkSession的初始化
    this.init()

    // spark为sparkSession的实例，已经在init()中完成初始化，可以直接通过this.fire或this.spark方式调用
    this.fire.sql("use tmp")
    this.fire.sql("show tables").show(100, false)

    this.fire.stop()
  }
}
