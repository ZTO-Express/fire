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

package com.zto.fire.flink.conf

import com.zto.fire.common.util.PropUtils

/**
 * flink相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:55
 */
private[fire] object FireFlinkConf {
  lazy val FLINK_AUTO_GENERATE_UID_ENABLE = "flink.auto.generate.uid.enable"
  lazy val FLINK_AUTO_TYPE_REGISTRATION_ENABLE = "flink.auto.type.registration.enable"
  lazy val FLINK_FORCE_AVRO_ENABLE = "flink.force.avro.enable"
  lazy val FLINK_FORCE_KRYO_ENABLE = "flink.force.kryo.enable"
  lazy val FLINK_GENERIC_TYPES_ENABLE = "flink.generic.types.enable"
  lazy val FLINK_OBJECT_REUSE_ENABLE = "flink.object.reuse.enable"
  lazy val FLINK_AUTO_WATERMARK_INTERVAL = "flink.auto.watermark.interval"
  lazy val FLINK_CLOSURE_CLEANER_LEVEL = "flink.closure.cleaner.level"
  lazy val FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT = "flink.default.input.dependency.constraint"
  lazy val FLINK_EXECUTION_MODE = "flink.execution.mode"
  lazy val FLINK_LATENCY_TRACKING_INTERVAL = "flink.latency.tracking.interval"
  lazy val FLINK_MAX_PARALLELISM = "flink.max.parallelism"
  lazy val FLINK_DEFAULT_PARALLELISM = "flink.default.parallelism"
  lazy val FLINK_TASK_CANCELLATION_INTERVAL = "flink.task.cancellation.interval"
  lazy val FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS = "flink.task.cancellation.timeout.millis"
  lazy val FLINK_USE_SNAPSHOT_COMPRESSION = "flink.use.snapshot.compression"
  lazy val FLINK_STREAM_BUFFER_TIMEOUT_MILLIS = "flink.stream.buffer.timeout.millis"
  lazy val FLINK_STREAM_NUMBER_EXECUTION_RETRIES = "flink.stream.number.execution.retries"
  lazy val FLINK_STREAM_TIME_CHARACTERISTIC = "flink.stream.time.characteristic"
  lazy val FLINK_DRIVER_CLASS_NAME = "flink.driver.class.name"
  lazy val FLINK_CLIENT_SIMPLE_CLASS_NAME = "flink.client.simple.class.name"
  lazy val FLINK_SQL_CONF_UDF_JARS = "flink.sql.conf.pipeline.jars"
  lazy val FLINK_SQL_LOG_ENABLE = "flink.sql.log.enable"

  // checkpoint相关配置项
  lazy val FLINK_STREAM_CHECKPOINT_INTERVAL = "flink.stream.checkpoint.interval"
  lazy val FLINK_STREAM_CHECKPOINT_MODE = "flink.stream.checkpoint.mode"
  lazy val FLINK_STREAM_CHECKPOINT_TIMEOUT = "flink.stream.checkpoint.timeout"
  lazy val FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT = "flink.stream.checkpoint.max.concurrent"
  lazy val FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN = "flink.stream.checkpoint.min.pause.between"
  lazy val FLINK_STREAM_CHECKPOINT_PREFER_RECOVERY = "flink.stream.checkpoint.prefer.recovery"
  lazy val FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER = "flink.stream.checkpoint.tolerable.failure.number"
  lazy val FLINK_STREAM_CHECKPOINT_EXTERNALIZED = "flink.stream.checkpoint.externalized"
  lazy val FLINK_SQL_WITH_REPLACE_MODE_ENABLE = "flink.sql_with.replaceMode.enable"

  // flink sql相关配置
  lazy val FLINK_SQL_CONF_PREFIX = "flink.sql.conf."
  // udf自动注册
  lazy val FLINK_SQL_UDF = "flink.sql.udf."
  lazy val FLINK_SQL_UDF_ENABLE = "flink.sql.udf.fireUdf.enable"
  lazy val FLINK_SQL_WITH_PREFIX = "flink.sql.with."

  lazy val sqlWithReplaceModeEnable = PropUtils.getBoolean(this.FLINK_SQL_WITH_REPLACE_MODE_ENABLE, false)
  lazy val autoGenerateUidEnable = PropUtils.getBoolean(this.FLINK_AUTO_GENERATE_UID_ENABLE, true)
  lazy val autoTypeRegistrationEnable = PropUtils.getBoolean(this.FLINK_AUTO_TYPE_REGISTRATION_ENABLE, true)
  lazy val forceAvroEnable = PropUtils.getBoolean(this.FLINK_FORCE_AVRO_ENABLE, false)
  lazy val forceKryoEnable = PropUtils.getBoolean(this.FLINK_FORCE_KRYO_ENABLE, false)
  lazy val genericTypesEnable = PropUtils.getBoolean(this.FLINK_GENERIC_TYPES_ENABLE, false)
  lazy val objectReuseEnable = PropUtils.getBoolean(this.FLINK_OBJECT_REUSE_ENABLE, false)
  lazy val autoWatermarkInterval = PropUtils.getLong(this.FLINK_AUTO_WATERMARK_INTERVAL, -1)
  lazy val closureCleanerLevel = PropUtils.getString(this.FLINK_CLOSURE_CLEANER_LEVEL)
  lazy val defaultInputDependencyConstraint = PropUtils.getString(this.FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT)
  lazy val executionMode = PropUtils.getString(this.FLINK_EXECUTION_MODE)
  lazy val latencyTrackingInterval = PropUtils.getLong(this.FLINK_LATENCY_TRACKING_INTERVAL, -1)
  lazy val maxParallelism = PropUtils.getInt(this.FLINK_MAX_PARALLELISM, 8)
  lazy val defaultParallelism = PropUtils.getInt(this.FLINK_DEFAULT_PARALLELISM, -1)
  lazy val taskCancellationInterval = PropUtils.getLong(this.FLINK_TASK_CANCELLATION_INTERVAL, -1)
  lazy val taskCancellationTimeoutMillis = PropUtils.getLong(this.FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS, -1)
  lazy val useSnapshotCompression = PropUtils.getBoolean(this.FLINK_USE_SNAPSHOT_COMPRESSION, false)
  lazy val streamBufferTimeoutMillis = PropUtils.getLong(this.FLINK_STREAM_BUFFER_TIMEOUT_MILLIS, -1)
  lazy val streamNumberExecutionRetries = PropUtils.getInt(this.FLINK_STREAM_NUMBER_EXECUTION_RETRIES, -1)
  lazy val streamTimeCharacteristic = PropUtils.getString(this.FLINK_STREAM_TIME_CHARACTERISTIC, "")
  lazy val sqlLogEnable = PropUtils.getBoolean(this.FLINK_SQL_LOG_ENABLE, false)

  // checkpoint相关配置项
  lazy val streamCheckpointInterval = PropUtils.getLong(this.FLINK_STREAM_CHECKPOINT_INTERVAL, -1)
  lazy val streamCheckpointMode = PropUtils.getString(this.FLINK_STREAM_CHECKPOINT_MODE, "EXACTLY_ONCE")
  lazy val streamCheckpointTimeout = PropUtils.getLong(this.FLINK_STREAM_CHECKPOINT_TIMEOUT, 600000L)
  lazy val streamCheckpointMaxConcurrent = PropUtils.getInt(this.FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, 1)
  lazy val streamCheckpointMinPauseBetween = PropUtils.getInt(this.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, 0)
  lazy val streamCheckpointPreferRecovery = PropUtils.getBoolean(this.FLINK_STREAM_CHECKPOINT_PREFER_RECOVERY, false)
  lazy val streamCheckpointTolerableTailureNumber = PropUtils.getInt(this.FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, 0)
  lazy val streamCheckpointExternalized = PropUtils.getString(this.FLINK_STREAM_CHECKPOINT_EXTERNALIZED, "RETAIN_ON_CANCELLATION")

  // flink sql相关配置
  lazy val flinkSqlConfig = PropUtils.sliceKeys(this.FLINK_SQL_CONF_PREFIX)
  // 用于自动注册udf jar包中的函数
  lazy val flinkUdfList = PropUtils.sliceKeys(this.FLINK_SQL_UDF)
  // 是否启用fire udf注册功能
  lazy val flinkUdfEnable = PropUtils.getBoolean(this.FLINK_SQL_UDF_ENABLE, true)
}
