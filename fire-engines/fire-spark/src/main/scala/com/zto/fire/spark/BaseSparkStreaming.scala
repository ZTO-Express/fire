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

package com.zto.fire.spark


import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.conf.{FireFrameworkConf, FireKafkaConf}
import com.zto.fire.common.enu.{ErrorCode, JobType, RequestMethod}
import com.zto.fire.common.util.{JSONUtils, KafkaUtils, PropUtils, ReflectionUtils}
import com.zto.fire.core.rest.RestCase
import com.zto.fire.spark.bean.RestartParams
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext, StreamingContextState}
import spark.{Request, Response}
import com.zto.fire._
import com.zto.fire.spark.conf.FireSparkConf


/**
 * Spark Streaming通用父接口
 * Created by ChengLong on 2018-03-28.
 */
trait BaseSparkStreaming extends BaseSpark {
  var checkPointDir: String = _
  var externalConf: RestartParams = _
  override val jobType = JobType.SPARK_STREAMING

  /**
   * 程序初始化方法，用于初始化必要的值
   *
   * @param batchDuration
   * Streaming每个批次间隔时间
   * @param isCheckPoint
   * 是否做checkpoint
   */
  def init(batchDuration: Long, isCheckPoint: Boolean): Unit = {
    this.init(batchDuration, isCheckPoint, null)
  }

  /**
   * 程序初始化方法，用于初始化必要的值
   *
   * @param batchDuration
   * Streaming每个批次间隔时间
   * @param isCheckPoint
   * 是否做checkpoint
   */
  def init(batchDuration: Long, isCheckPoint: Boolean, args: Array[String]): Unit = {
    this.init(batchDuration, isCheckPoint, null, args)
    if (FireFrameworkConf.jobAutoStart && this.ssc.getState() == StreamingContextState.INITIALIZED) this.fire.start
  }

  /**
   * 程序初始化方法，用于初始化必要的值
   *
   * @param batchDuration
   * Streaming每个批次间隔时间
   * @param isCheckPoint
   * 是否做checkpoint
   * @param conf
   * 传入自己构建的sparkConf对象，可以为空
   */
  def init(batchDuration: Long, isCheckPoint: Boolean, conf: SparkConf, args: Array[String]): Unit = {
    val tmpConf = buildConf(conf)
    if (this.sc == null) {
      // 添加streaming相关的restful接口，并启动
      this.init(tmpConf, args)
      this.restfulRegister
        .addRest(RestCase(RequestMethod.POST.toString, "/system/streaming/hotRestart", this.hotRestart))
        .startRestServer
    }
    // 判断是否为热重启，batchDuration优先级分别为 [ 代码<配置文件<热重启 ]
    this.batchDuration = SparkUtils.overrideBatchDuration(batchDuration, this.externalConf != null)
    if (!isCheckPoint) {
      if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
        // 重启SparkContext对象
        this.ssc = new StreamingContext(tmpConf, Seconds(Math.abs(this.batchDuration)))
        this.sc = this.ssc.sparkContext
      } else {
        this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(this.batchDuration)))
      }
      val rememberTime = FireSparkConf.streamingRemember
      if (rememberTime > 0) this.ssc.remember(Milliseconds(Math.abs(rememberTime)))
      SparkSingletonFactory.setStreamingContext(this.ssc)
      this.processAll
    } else {
      this.checkPointDir = FireSparkConf.chkPointDirPrefix + this.appName
      this.ssc = StreamingContext.getOrCreate(this.checkPointDir, createStreamingContext _)

      // 初始化Streaming
      def createStreamingContext(): StreamingContext = {
        tmpConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
        if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
          // 重启SparkContext对象
          this.ssc = new StreamingContext(tmpConf, Seconds(Math.abs(this.batchDuration)))
          this.sc = this.ssc.sparkContext
        } else {
          this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(this.batchDuration)))
        }
        this.ssc.checkpoint(checkPointDir)
        SparkSingletonFactory.setStreamingContext(this.ssc)
        this.processAll
        this.ssc
      }
    }
    this._conf = tmpConf
  }

  /**
   * 构建内部使用的SparkConf对象
   */
  override def buildConf(conf: SparkConf = null): SparkConf = {
    val tmpConf = super.buildConf(conf)

    // 若重启SparkContext对象，则设置restful传递过来的新的配置信息
    if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
      if (this.externalConf.getSparkConf != null && this.externalConf.getSparkConf.size() > 0) {
        tmpConf.setAll(this.externalConf.getSparkConf)
      }
    }

    tmpConf
  }

  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf: Unit = {
    PropUtils.load(FireFrameworkConf.SPARK_STREAMING_CONF_FILE)
  }

  /**
   * 初始化SparkSession与StreamingContext，默认批次时间为30s
   * 批次时间可通过子类复写main方法实现或通过在配置文件中指定：spark.streaming.batch.duration=30
   */
  override def main(args: Array[String]): Unit = {
    val batchDuration = this.conf.getLong("spark.streaming.batch.duration", 10)
    val ck = this.conf.getBoolean("spark.streaming.receiver.writeAheadLog.enable", false)
    this.init(batchDuration, ck, args)
  }

  /**
   * Streaming的处理过程强烈建议放到process中，保持风格统一
   * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
   * 1. 开启checkpoint
   * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
   */
  override def process: Unit = {
    require(this.checkPointDir == null, "当开启checkPoint机制时，必须将对接kafka的代码写在process方法内")
    require(this.externalConf == null, "当需要使用热重启功能时，必须将对接kafka的代码写在process方法内")
  }

  /**
   * kafka配置信息
   *
   * @param groupId
   * 消费组
   * @param offset
   * offset位点，smallest、largest，默认为largest
   * @return
   * kafka相关配置
   */
  @Deprecated
  def kafkaParams(groupId: String = this.appName, kafkaBrokers: String = null, offset: String = FireKafkaConf.offsetLargest, autoCommit: Boolean = false, keyNum: Int = 1): Map[String, Object] = {
    KafkaUtils.kafkaParams(null, groupId, kafkaBrokers, offset, autoCommit, keyNum)
  }

  /**
   * 用于重置StreamingContext（仅支持batch时间的修改）
   *
   * @return
   * 响应结果
   */
  @Rest("/system/streaming/hotRestart")
  def hotRestart(request: Request, response: Response): AnyRef = {
    val json = request.body
    try {
      this.logger.info(s"Ip address ${request.ip()} request /system/streaming/hotRestart")
      this.externalConf = JSONUtils.parseObject[RestartParams](json)
      new Thread(new Runnable {
        override def run(): Unit = {
          ssc.stop(externalConf.isRestartSparkContext, externalConf.isStopGracefully)
          init(externalConf.getBatchDuration, externalConf.isCheckPoint)
        }
      }).start()

      this.logger.info(s"[hotRestart] 执行热重启成功：duration=${this.externalConf.getBatchDuration} json=$json", "rest")
      ResultMsg.buildSuccess(s"执行热重启成功：duration=${this.externalConf.getBatchDuration}", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logger.error(s"[hotRestart] 执行热重启失败：json=$json", e)
        ResultMsg.buildError("执行热重启失败", ErrorCode.ERROR)
      }
    }
  }

}
