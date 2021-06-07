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

package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils

/**
 * HDFS配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:07
 */
private[fire] object FireHDFSConf {
  // 是否启用高可用
  lazy val HDFS_HA = "hdfs.ha.enable"
  lazy val HDFS_HA_PREFIX = "hdfs.ha.conf."


  // 配置是否启用hdfs HA
  lazy val hdfsHAEnable = PropUtils.getBoolean(this.HDFS_HA, true)

  /**
   * 读取HDFS高可用相关配置信息
   */
  def hdfsHAConf: Map[String, String] = {
    if (FireHDFSConf.hdfsHAEnable) {
      PropUtils.sliceKeys(s"${this.HDFS_HA_PREFIX}${FireHiveConf.hiveCluster}.")
    } else Map.empty
  }
}