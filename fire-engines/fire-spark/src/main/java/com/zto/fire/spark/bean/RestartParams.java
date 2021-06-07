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

package com.zto.fire.spark.bean;

import java.util.Map;

/**
 * 重启streaming参数
 * {"batchDuration":10,"restartSparkContext":false,"stopGracefully": false,"sparkConf":{"spark.streaming.concurrentJobs":"2"}}
 * @author ChengLong 2019-5-5 16:57:49
 */
public class RestartParams {
    // 批次时间（秒）
    private long batchDuration;
    // 是否重启SparkContext对象
    private boolean restartSparkContext;
    // 是否等待数据全部处理完成再重启
    private boolean stopGracefully;
    // 是否做checkPoint
    private boolean isCheckPoint;
    // 附加的conf信息
    private Map<String, String> sparkConf;

    public long getBatchDuration() {
        return batchDuration;
    }

    public void setBatchDuration(long batchDuration) {
        this.batchDuration = batchDuration;
    }

    public boolean isRestartSparkContext() {
        return restartSparkContext;
    }

    public void setRestartSparkContext(boolean restartSparkContext) {
        this.restartSparkContext = restartSparkContext;
    }

    public Map<String, String> getSparkConf() {
        return sparkConf;
    }

    public void setSparkConf(Map<String, String> sparkConf) {
        this.sparkConf = sparkConf;
    }

    public RestartParams() {
    }

    public boolean isStopGracefully() {
        return stopGracefully;
    }

    public void setStopGracefully(boolean stopGracefully) {
        this.stopGracefully = stopGracefully;
    }

    public boolean isCheckPoint() {
        return isCheckPoint;
    }

    public void setCheckPoint(boolean checkPoint) {
        isCheckPoint = checkPoint;
    }

    public RestartParams(long batchDuration, boolean restartSparkContext, boolean stopGracefully, boolean isCheckPoint, Map<String, String> sparkConf) {
        this.batchDuration = batchDuration;
        this.restartSparkContext = restartSparkContext;
        this.stopGracefully = stopGracefully;
        this.isCheckPoint = isCheckPoint;
        this.sparkConf = sparkConf;
    }
}
