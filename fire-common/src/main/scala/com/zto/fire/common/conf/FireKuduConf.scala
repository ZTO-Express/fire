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
 * kudu & impala相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:59
 */
private[fire] object FireKuduConf {
  lazy val KUDU_MASTER_URL = "kudu.master"
  lazy val IMPALA_CONNECTION_URL_KEY = "impala.connection.url"
  lazy val IMPALA_JDBC_DRIVER_NAME_KEY = "impala.jdbc.driver.class.name"
  lazy val IMPALA_DAEMONS_URL = "impala.daemons.url"

  lazy val kuduMaster = PropUtils.getString(this.KUDU_MASTER_URL)
  lazy val impalaConnectionUrl: String = PropUtils.getString(this.IMPALA_CONNECTION_URL_KEY)
  lazy val impalaJdbcDriverName: String = PropUtils.getString(this.IMPALA_JDBC_DRIVER_NAME_KEY)
  lazy val impalaDaemons: String = PropUtils.getString(this.IMPALA_DAEMONS_URL, "")
}