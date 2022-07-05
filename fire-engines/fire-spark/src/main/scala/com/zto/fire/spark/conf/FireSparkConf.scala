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

package com.zto.fire.spark.conf

import com.zto.fire.common.util.PropUtils

/**
 * Spark引擎相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:57
 */
private[fire] object FireSparkConf {
  lazy val SPARK_APP_NAME = "spark.appName"
  lazy val SPARK_LOCAL_CORES = "spark.local.cores"
  lazy val SPARK_LOG_LEVEL = "spark.log.level"
  lazy val SPARK_SAVE_MODE = "spark.saveMode"
  lazy val SPARK_PARALLELISM = "spark.parallelism"
  lazy val SPARK_CHK_POINT_DIR = "spark.chkpoint.dir"
  lazy val SPARK_SQL_EXTENSIONS_ENABLE = "spark.fire.sql.extensions.enable"

  // spark datasource v2 api中的options配置key前缀
  lazy val SPARK_DATASOURCE_OPTIONS_PREFIX = "spark.datasource.options."
  lazy val SPARK_DATASOURCE_FORMAT = "spark.datasource.format"
  lazy val SPARK_DATSOURCE_SAVE_MODE = "spark.datasource.saveMode"
  // 用于dataFrame.write.format.save()参数
  lazy val SPARK_DATASOURCE_SAVE_PARAM = "spark.datasource.saveParam"
  lazy val SPARK_DATASOURCE_IS_SAVE_TABLE = "spark.datasource.isSaveTable"
  // 用于spark.read.format.load()参数
  lazy val SPARK_DATASOURCE_LOAD_PARAM = "spark.datasource.loadParam"

  // spark 默认的checkpoint地址
  lazy val sparkChkPointDir = "hdfs://nameservice1/user/spark/ckpoint/"
  // spark streaming批次时间
  lazy val SPARK_STREAMING_BATCH_DURATION = "spark.streaming.batch.duration"
  // spark streaming的remember时间，-1表示不生效(ms)
  lazy val SPARK_STREAMING_REMEMBER = "spark.streaming.remember"
  // 当stage失败多少个时退出整个SparkSession
  lazy val SPARK_FIRE_STAGE_MAXFAILURES = "spark.fire.stage.maxFailures"

  // spark streaming的remember时间，-1表示不生效(ms)
  def streamingRemember: Long = PropUtils.getLong(this.SPARK_STREAMING_REMEMBER, -1)
  lazy val appName = PropUtils.getString(this.SPARK_APP_NAME, "")
  lazy val localCores = PropUtils.getString(this.SPARK_LOCAL_CORES, "*")
  lazy val logLevel = PropUtils.getString(this.SPARK_LOG_LEVEL, "info").toUpperCase
  lazy val saveMode = PropUtils.getString(this.SPARK_SAVE_MODE, "Append")
  lazy val parallelism = PropUtils.getInt(this.SPARK_PARALLELISM, 200)
  lazy val chkPointDirPrefix = PropUtils.getString(this.SPARK_CHK_POINT_DIR, this.sparkChkPointDir)
  lazy val confBathDuration = PropUtils.getInt(this.SPARK_STREAMING_BATCH_DURATION, -1)
  // 是否启用spark sql解析器扩展
  lazy val sqlExtensionsEnable = PropUtils.getBoolean(this.SPARK_SQL_EXTENSIONS_ENABLE, true)

  /**
   * spark datasource api中的format参数
   */
  def datasourceFormat(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DATASOURCE_FORMAT, "", keyNum)

  /**
   * spark datasource api中的saveMode参数
   */
  def datasourceSaveMode(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DATSOURCE_SAVE_MODE, "Append", keyNum)

  /**
   * spark datasource api中的save方法参数
   */
  def datasourceSaveParam(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DATASOURCE_SAVE_PARAM, "", keyNum)

  /**
   * spark datasource api中的isSaveTable方法
   */
  def datasourceIsSaveTable(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DATASOURCE_IS_SAVE_TABLE, "", keyNum)

  /**
   * spark datasource api中的load方法参数
   */
  def datasourceLoadParam(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DATASOURCE_LOAD_PARAM, "", keyNum)

  /**
   * 当stage失败次数大于该值时SparkSession退出
   */
  def stageMaxFailures: Int = PropUtils.getInt(this.SPARK_FIRE_STAGE_MAXFAILURES, -1)
}
